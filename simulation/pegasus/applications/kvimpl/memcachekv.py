"""
memcachekv.py: Memcache style distributed key-value store.
"""

import random
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
    def __init__(self, req_id, result, value):
        super().__init__(kv.REQ_ID_LEN + kv.RES_LEN + len(value))
        self.req_id = req_id
        self.result = result
        self.value = value


class MemcacheKVConfiguration(pegasus.config.Configuration):
    """
    Abstract configuration class. Subclass of ``MemcacheKVConfiguration``
    should implement ``key_to_nodes``.
    """
    def __init__(self, cache_nodes, db_node):
        super().__init__()
        self.cache_nodes = cache_nodes
        self.db_node = db_node

    def key_to_nodes(self, key):
        """
        Return node/nodes the ``key`` is mapped to.
        """
        raise NotImplementedError


class StaticConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node):
        super().__init__(cache_nodes, db_node)

    def key_to_nodes(self, key):
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

    class LoadReport(object):
        def __init__(self):
            self.key_request_rates = [] # list of KeyRequestRate

    def __init__(self, cache_nodes, db_node, max_request_rate):
        super().__init__(cache_nodes, db_node)
        self.key_node_map = {} # key -> nodes
        self.agg_key_request_rate = {}
        self.max_request_rate = max_request_rate

    def key_to_nodes(self, key):
        return self.key_node_map.setdefault(key, [self.cache_nodes[hash(key) % len(self.cache_nodes)]])

    def report_load(self, node, report):
        for krr in report.key_request_rates:
            rate = self.agg_key_request_rate.get(krr.key, 0)
            self.agg_key_request_rate[krr.key] = rate + krr.request_rate

    def rebalance_load(self):
        # Construct sorted key request rates and node request rates
        sorted_krr = SortedList()
        sorted_nrr = SortedList()
        for item in self.agg_key_request_rate.items():
            sorted_krr.add(self.KeyRequestRate(item[0], item[1]))
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
        dest_nodes = self._config.key_to_nodes(op.key)
        pending_req = self.PendingRequest(operation = op, time = time)
        msg = MemcacheKVRequest(src = self._node,
                                req_id = self._next_req_id,
                                operation = op)
        if op.op_type == kv.Operation.Type.GET:
            self._node.send_message(msg, random.choice(dest_nodes), time)
        else:
            for node in dest_nodes:
                self._node.send_message(msg, node, time)
            pending_req.expected_acks = len(dest_nodes)
        self._pending_requests[self._next_req_id] = pending_req
        self._next_req_id += 1

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVReply):
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

    def execute(self, end_time):
        """
        Override KV's execute method.
        """
        pass

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVRequest):
            result, value = self._execute_op(message.operation)
            reply = MemcacheKVReply(req_id = message.req_id,
                                    result = result,
                                    value = value)
            self._node.send_message(reply, message.src, time)
        else:
            raise ValueError("Invalid message type")
