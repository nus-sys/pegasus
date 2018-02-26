"""
memcachekv.py: Memcache style distributed key-value store.
"""

import random
import enum
from sortedcontainers import SortedList

import pegasus.message
import pegasus.config
import pegasus.applications.kv as kv

class MemcacheKVRequest(pegasus.message.Message):
    """
    Request message used by MemcacheKV.
    """
    def __init__(self, src, req_id, operation):
        super().__init__(kv.REQ_ID_LEN + operation.len())
        self.src = src
        self.req_id = req_id
        self.operation = operation


class MemcacheKVReply(pegasus.message.Message):
    """
    Reply message used by MemcacheKV.
    """
    def __init__(self, src, req_id, result, value):
        super().__init__(kv.REQ_ID_LEN + kv.RES_LEN + len(value))
        self.src = src
        self.req_id = req_id
        self.result = result
        self.value = value


class WriteMode(enum.Enum):
    ANYNODE = 1
    UPDATE = 2
    INVALIDATE = 3

class MemcacheKVConfiguration(pegasus.config.Configuration):
    """
    Abstract configuration class. Subclass of ``MemcacheKVConfiguration``
    should implement ``key_to_nodes``.
    """
    def __init__(self, cache_nodes, db_node, write_type):
        super().__init__()
        self.cache_nodes = cache_nodes
        self.db_node = db_node
        self.write_type = write_type
        self.report_load = False
        self.report_interval = 0

    def run(self, end_time):
        pass

    def key_to_nodes(self, key, op_type):
        """
        Return node/nodes the ``key`` is mapped to.
        """
        raise NotImplementedError

    def report_op_send(self, node):
        """
        (Client) reporting that it is sending an op to ``node``.
        """
        pass

    def report_op_receive(self, node):
        """
        (Client) reporting that it has received a reply from ``node``.
        """
        pass


class StaticConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node, write_type):
        super().__init__(cache_nodes, db_node, write_type)

    def key_to_nodes(self, key, op_type):
        return [self.cache_nodes[hash(key) % len(self.cache_nodes)]]


class LoadBalanceConfig(MemcacheKVConfiguration):
    class KeyRequestRate(object):
        def __init__(self, key, request_rate):
            self.key = key
            self.request_rate = request_rate

        def __eq__(self, other):
            if isinstance(other, KeyRequestRate):
                return self.key == other.key and self.request_rate == other.request_rate
            else:
                return False

        def __lt__(self, other):
            return self.request_rate < other.request_rate

    class NodeRequestRate(object):
        def __init__(self, id, request_rate):
            self.id = id
            self.request_rate = request_rate

        def __eq__(self, other):
            if isinstance(other, NodeRequestRate):
                return self.id == other.id and self.request_rate == other.request_rate
            else:
                return False

        def __lt__(self, other):
            return self.request_rate < other.request_rate

    def __init__(self, cache_nodes, db_node, write_type, max_request_rate, report_interval):
        super().__init__(cache_nodes, db_node, write_type)
        self.key_node_map = {} # key -> nodes
        self.agg_key_request_rate = {}
        self.max_request_rate = max_request_rate
        self.report_load = True
        self.report_interval = report_interval
        self.last_load_rebalance_time = 0

    def run(self, end_time):
        if end_time - self.last_load_rebalance_time >= self.report_interval:
            self.collect_load(end_time - self.last_load_rebalance_time)
            self.rebalance_load()
            self.last_load_rebalance_time = end_time

    def key_to_nodes(self, key, op_type):
        return self.key_node_map.setdefault(key, [self.cache_nodes[hash(key) % len(self.cache_nodes)]])

    def collect_load(self, interval):
        for node in self.cache_nodes:
            for key, count in node._app._key_request_counter.items():
                rate = self.agg_key_request_rate.get(key, 0)
                self.agg_key_request_rate[key] = rate + round(count / (interval / 1000000))
            node._app._key_request_counter.clear()

    def rebalance_load(self):
        # Construct sorted key request rates and node request rates
        sorted_krr = SortedList()
        sorted_nrr = SortedList()
        for key, rate in self.agg_key_request_rate.items():
            sorted_krr.add(self.KeyRequestRate(key, rate))
        for node in self.cache_nodes:
            sorted_nrr.add(self.NodeRequestRate(node.id, 0))

        # Try to add the most loaded key to the least loaded node.
        # If not possible, replicate the key.
        while len(sorted_krr) > 0:
            krr = sorted_krr.pop()
            nrr = sorted_nrr.pop(0)

            if nrr.request_rate + krr.request_rate <= self.max_request_rate:
                self.key_node_map[krr.key] = [self.cache_nodes[nrr.id]]
                nrr.request_rate += krr.request_rate
                sorted_nrr.add(nrr)
            else:
                # Replicate the key until the request rate fits (or reaches max replication)
                nrrs = [nrr]
                while len(sorted_nrr) > 0:
                    nrrs.append(sorted_nrr.pop(0))
                    request_rate = krr.request_rate // len(nrrs)
                    fit = True
                    for nrr in nrrs:
                        if nrr.request_rate + request_rate > self.max_request_rate:
                            fit = False
                            break
                    if fit:
                        break
                # Update sorted_nrr and key node map
                nodes = []
                for nrr in nrrs:
                    nodes.append(self.cache_nodes[nrr.id])
                    nrr.request_rate += (krr.request_rate // len(nrrs))
                    sorted_nrr.add(nrr)
                self.key_node_map[krr.key] = nodes

        # Clear aggregate request rate
        self.agg_key_request_rate.clear()


class BoundedLoadConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node, write_type, c):
        super().__init__(cache_nodes, db_node, write_type)
        self.c = c
        self.outstanding_requests = {} # node id -> number of outstanding requests
        self.key_node_map = {} # key -> nodes
        self.write_type = write_type
        for node in self.cache_nodes:
            self.outstanding_requests[node.id] = 0

    def key_hash(self, key):
        return hash(key)

    def key_to_nodes(self, key, op_type):
        total_load = sum(self.outstanding_requests.values())
        expected_load = (self.c * total_load) / len(self.cache_nodes)
        next_node_id = self.key_hash(key) % len(self.cache_nodes)
        while self.outstanding_requests[next_node_id] > expected_load:
            next_node_id = (next_node_id + 1) % len(self.cache_nodes)
        return [self.cache_nodes[next_node_id]]

    def report_op_send(self, node):
        self.outstanding_requests[node.id] += 1

    def report_op_receive(self, node):
        self.outstanding_requests[node.id] -= 1


class MemcacheKVClient(kv.KV):
    """
    Implementation of a memcache style distributed key-value store client.
    """
    class PendingRequest(kv.KV.PendingRequest):
        def __init__(self, operation, time):
            super().__init__(operation, time)
            self.received_acks = 0
            self.expected_acks = 0

    def __init__(self, generator, stats):
        super().__init__(generator, stats)

    def _execute(self, op, time):
        """
        Always send the operation to a remote node. Client nodes
        in a MemcacheKV are stateless, and do not store kv pairs.
        If the key is replicated on multiple cache nodes
        (key_to_nodes returns multiple nodes), pick one node in
        random for GET requests, and send to all replicated nodes
        for PUT and DEL requests.
        """
        dest_nodes = self._config.key_to_nodes(op.key, op.op_type)
        pending_req = self.PendingRequest(operation = op, time = time)
        msg = MemcacheKVRequest(src = self._node,
                                req_id = self._next_req_id,
                                operation = op)
        if op.op_type == kv.Operation.Type.GET:
            node = random.choice(dest_nodes)
            self._node.send_message(msg, node, time)
            self._config.report_op_send(node)
        else:
            for node in dest_nodes:
                self._node.send_message(msg, node, time)
                self._config.report_op_send(node)
            pending_req.expected_acks = len(dest_nodes)
        self._pending_requests[self._next_req_id] = pending_req
        self._next_req_id += 1

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVReply):
            self._config.report_op_receive(message.src)
            request = self._pending_requests[message.req_id]
            if request.operation.op_type == kv.Operation.Type.GET:
                self._complete_request(message.req_id, message.result, time)
            elif request.operation.op_type == kv.Operation.Type.PUT or \
                request.operation.op_type == kv.Operation.Type.DEL:
                request.received_acks += 1
                if request.received_acks >= request.expected_acks:
                    self._complete_request(message.req_id, kv.Result.OK, time)
        else:
            raise ValueError("Invalid message type")

    def _complete_request(self, req_id, result, time):
        request = self._pending_requests.pop(req_id)
        self._stats.report_op(request.operation.op_type,
                              time - request.time,
                              result == kv.Result.OK)

class MemcacheKVServer(kv.KV):
    """
    Implementation of a memcache style distributed key-value store server.
    """
    def __init__(self, generator, stats):
        super().__init__(generator, stats)
        self._key_request_counter = {}
        self._last_load_report_time = 0

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVRequest):
            if self._config.report_load:
                count = self._key_request_counter.get(message.operation.key, 0) + 1
                self._key_request_counter[message.operation.key] = count
            result, value = self._execute_op(message.operation)
            reply = MemcacheKVReply(src = self._node,
                                    req_id = message.req_id,
                                    result = result,
                                    value = value)
            self._node.send_message(reply, message.src, time)
        else:
            raise ValueError("Invalid message type")
