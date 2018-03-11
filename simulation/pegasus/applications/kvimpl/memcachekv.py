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
    class Type(enum.Enum):
        CLIENT = 1
        MIGRATION = 2

    def __init__(self, type, src, req_id, operation):
        super().__init__(kv.REQ_ID_LEN + operation.len())
        self.type = type
        self.src = src
        self.req_id = req_id
        self.operation = operation
        self.migration_dests = None


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


class MappedNodes(object):
    def __init__(self, dest_nodes, migration_nodes):
        self.dest_nodes = dest_nodes
        self.migration_nodes = migration_nodes


class KeyRate(object):
    def __init__(self, count=0, time=0):
        self.count = count
        self.time = time

    def rate(self):
        if self.time == 0 or self.count <= 1:
            return 0
        return self.count / (self.time / 1000000)


class MemcacheKVConfiguration(pegasus.config.Configuration):
    """
    Abstract configuration class. Subclass of ``MemcacheKVConfiguration``
    should implement ``key_to_nodes``.
    """
    def __init__(self, cache_nodes, db_node, write_mode):
        super().__init__()
        self.cache_nodes = cache_nodes
        self.db_node = db_node
        self.write_mode = write_mode
        self.report_load = False
        self.report_interval = 0

    def run(self, end_time):
        pass

    def key_to_nodes(self, key, op_type):
        """
        Return node/nodes the ``key`` is mapped to.
        Return type should be ``MappedNodes``.
        """
        raise NotImplementedError

    def report_op_send(self, node, op, time):
        """
        (Client) reporting that it is sending an op to ``node``.
        """
        pass

    def report_op_receive(self, node):
        """
        (Client) reporting that it has received a reply from ``node``.
        """
        pass

    def report_migration(self, key, src, dest):
        """
        (Server) reporting that ``key`` has migrated from ``src`` to ``dest``.
        """
        pass


class StaticConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node, write_mode):
        super().__init__(cache_nodes, db_node, write_mode)

    def key_to_nodes(self, key, op_type):
        return MappedNodes([self.cache_nodes[hash(key) % len(self.cache_nodes)]],
                           None)


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

    def __init__(self, cache_nodes, db_node, write_mode, max_request_rate, report_interval):
        super().__init__(cache_nodes, db_node, write_mode)
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
        return MappedNodes(self.key_node_map.setdefault(key, [self.cache_nodes[hash(key) % len(self.cache_nodes)]]),
                           None)

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
    def __init__(self, cache_nodes, db_node, write_mode, c):
        super().__init__(cache_nodes, db_node, write_mode)
        self.c = c
        self.outstanding_requests = {} # node id -> number of outstanding requests
        self.key_node_map = {} # key -> node
        for node in self.cache_nodes:
            self.outstanding_requests[node.id] = 0

    def key_hash(self, key):
        return hash(key)

    def key_to_nodes(self, key, op_type):
        if op_type == kv.Operation.Type.DEL or op_type == kv.Operation.Type.PUT:
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            return MappedNodes([self.cache_nodes[node_id]], None)
        else:
            # For GET requests, migrate the key if the mapped node is
            # above the bounded load
            total_load = sum(self.outstanding_requests.values())
            expected_load = (self.c * total_load) / len(self.cache_nodes)
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            if self.outstanding_requests[node_id] <= expected_load:
                return MappedNodes([self.cache_nodes[node_id]], None)

            # Current mapped node is over-loaded, find the next
            # node that is below the bounded load (migrate key)
            next_node_id = (node_id + 1) % len(self.cache_nodes)
            while self.outstanding_requests[next_node_id] > expected_load:
                next_node_id = (next_node_id + 1) % len(self.cache_nodes)
            assert node_id != next_node_id
            self.key_node_map[key] = next_node_id
            return MappedNodes([self.cache_nodes[node_id]],
                               [self.cache_nodes[next_node_id]])

    def report_op_send(self, node, op, time):
        self.outstanding_requests[node.id] += 1

    def report_op_receive(self, node):
        self.outstanding_requests[node.id] -= 1


class BoundedIPLoadConfig(MemcacheKVConfiguration):
    class Mode(enum.Enum):
        ILOAD = 1
        PLOAD = 2
        IPLOAD = 3

    def __init__(self, cache_nodes, db_node, write_mode, c, mode):
        super().__init__(cache_nodes, db_node, write_mode)
        self.c = c
        self.mode = mode
        self.key_node_map = {} # key -> node id
        self.key_rates = {} # key -> KeyRate
        self.iloads = {} # node id -> instantaneous load
        self.ploads = {} # node id -> projected load
        for node in self.cache_nodes:
            self.iloads[node.id] = 0
            self.ploads[node.id] = 0

    def key_hash(self, key):
        return hash(key)

    def key_to_nodes(self, key, op_type):
        if op_type == kv.Operation.Type.DEL or op_type == kv.Operation.Type.PUT:
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            return MappedNodes([self.cache_nodes[node_id]], None)
        else:
            # For GET requests, migrate the key if the mapped node is
            # exceeding the bounded iload and/or the bounded pload,
            # depending on the mode.
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            total_iload = sum(self.iloads.values())
            expected_iload = (self.c * total_iload) / len(self.cache_nodes)
            total_pload = sum(self.ploads.values())
            expected_pload = (self.c * total_pload) / len(self.cache_nodes)

            if self.mode == self.Mode.ILOAD or self.mode == self.Mode.IPLOAD:
                if self.iloads[node_id] <= expected_iload:
                    return MappedNodes([self.cache_nodes[node_id]], None)

            if self.mode == self.Mode.PLOAD or self.mode == self.Mode.IPLOAD:
                if self.ploads[node_id] <= expected_pload:
                    return MappedNodes([self.cache_nodes[node_id]], None)

            # Current mapped node is overloaded, find a node
            # to migrate to.
            if self.mode == self.Mode.ILOAD:
                next_node_id = min(self.iloads, key=self.iloads.get)
            elif self.mode == self.Mode.PLOAD:
                next_node_id = min(self.ploads, key=self.ploads.get)
            elif self.mode == self.Mode.IPLOAD:
                # For IPLOAD, we need to find a node that has both
                # iload and pload below the bounded load.
                node_found = False
                for next_node_id in sorted(self.ploads, key=self.ploads.get):
                    if self.ploads[next_node_id] > expected_pload:
                        break
                    if self.iloads[next_node_id] <= expected_iload:
                        node_found = True
                        break
                if not node_found:
                    return MappedNodes([self.cache_nodes[node_id]], None)

            assert node_id != next_node_id
            self.key_node_map[key] = next_node_id
            # Update ploads on both nodes
            key_rate = self.key_rates.get(key, KeyRate())
            self.ploads[node_id] -= key_rate.rate()
            self.ploads[next_node_id] += key_rate.rate()

            return MappedNodes([self.cache_nodes[node_id]],
                               [self.cache_nodes[next_node_id]])

    def report_op_send(self, node, op, time):
        self.iloads[node.id] += 1
        key_rate = self.key_rates.setdefault(op.key, KeyRate())
        old_rate = key_rate.rate()
        key_rate.count += 1
        key_rate.time = time
        node_id = self.key_node_map.get(op.key, self.key_hash(op.key) % len(self.cache_nodes))
        self.ploads[node_id] += (key_rate.rate() - old_rate)

    def report_op_receive(self, node):
        self.iloads[node.id] -= 1


class BoundedAverageLoadConfig(MemcacheKVConfiguration):
    class AverageLoad(object):
        def __init__(self):
            self.count = 0
            self.time = 0

        def load(self):
            if self.time == 0 or self.count <= 1:
                return 0
            return self.count / (self.time / 1000000)

    def __init__(self, cache_nodes, db_node, write_mode, c):
        super().__init__(cache_nodes, db_node, write_mode)
        self.c = c
        self.key_node_map = {} # key -> node id
        self.average_load = {} # node id -> AverageLoad
        for node in self.cache_nodes:
            self.average_load[node.id] = self.AverageLoad()

    def key_hash(self, key):
        return hash(key)

    def key_to_nodes(self, key, op_type):
        if op_type == kv.Operation.Type.DEL or op_type == kv.Operation.Type.PUT:
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            return MappedNodes([self.cache_nodes[node_id]], None)
        else:
            # For GET requests, migrate the key if the mapped node is
            # exceeding bounded average load
            node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
            total_load = sum(item.load() for item in self.average_load.values())
            expected_load = (self.c * total_load) / len(self.cache_nodes)
            if self.average_load[node_id].load() <= expected_load:
                return MappedNodes([self.cache_nodes[node_id]], None)

            # Current mapped node is overloaded, migrate the key to
            # the node with the lowest average load
            next_node_id = min(self.average_load, key=lambda x: self.average_load.get(x).load())

            assert node_id != next_node_id
            self.key_node_map[key] = next_node_id

            return MappedNodes([self.cache_nodes[node_id]],
                               [self.cache_nodes[next_node_id]])

    def report_op_send(self, node, op, time):
        self.average_load[node.id].count += 1
        self.average_load[node.id].time = time


class RoutingConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node, write_mode, c):
        super().__init__(cache_nodes, db_node, write_mode)
        self.c = c
        self.key_node_map = {} # key -> node id
        self.key_rates = {} # key -> KeyRate
        self.iloads = {} # node id -> instantaneous load
        self.ploads = {} # node id -> projected load
        for node in self.cache_nodes:
            self.iloads[node.id] = 0
            self.ploads[node.id] = 0

    def key_hash(self, key):
        return hash(key)

    def key_to_nodes(self, key, op_type):
        node_id = self.key_node_map.get(key, self.key_hash(key) % len(self.cache_nodes))
        return MappedNodes([self.cache_nodes[node_id]], None)

    def report_op_send(self, node, op, time):
        self.iloads[node.id] += 1
        key_rate = self.key_rates.setdefault(op.key, KeyRate())
        old_rate = key_rate.rate()
        key_rate.count += 1
        key_rate.time = time
        node_id = self.key_node_map.get(op.key, self.key_hash(op.key) % len(self.cache_nodes))
        self.ploads[node_id] += (key_rate.rate() - old_rate)

    def report_op_receive(self, node):
        self.iloads[node.id] -= 1

    def report_migration(self, key, src, dest):
        self.key_node_map[key] = dest.id


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
        for DEL requests. PUT request handling depends on the
        write_mode.
        """
        mapped_nodes = self._config.key_to_nodes(op.key, op.op_type)
        pending_req = self.PendingRequest(operation = op, time = time)
        msg = MemcacheKVRequest(type = MemcacheKVRequest.Type.CLIENT,
                                src = self._node,
                                req_id = self._next_req_id,
                                operation = op)
        if op.op_type == kv.Operation.Type.GET:
            node = random.choice(mapped_nodes.dest_nodes)
            if mapped_nodes.migration_nodes is not None:
                msg.migration_dests = mapped_nodes.migration_nodes
            self._node.send_message(msg, node, time)
            self._config.report_op_send(node, op, time)
        elif op.op_type == kv.Operation.Type.PUT:
            write_nodes = []
            inval_nodes = []
            if self._config.write_mode == WriteMode.ANYNODE:
                write_nodes = [random.choice(mapped_nodes.dest_nodes)]
            elif self._config.write_mode == WriteMode.UPDATE:
                write_nodes = mapped_nodes.dest_nodes
            elif self._config.write_mode == WriteMode.INVALIDATE:
                write_nodes = mapped_nodes.dest_nodes[:1]
                inval_nodes = mapped_nodes.dest_nodes[1:]
            else:
                raise ValueError("Invalid write mode")
            for node in write_nodes:
                self._node.send_message(msg, node, time)
                self._config.report_op_send(node, op, time)
            inval_msg = MemcacheKVRequest(type = MemcacheKVRequest.Type.CLIENT,
                                          src = self._node,
                                          req_id = self._next_req_id,
                                          operation = kv.Operation(op_type = kv.Operation.Type.DEL,
                                                                   key = op.key))
            for node in inval_nodes:
                self._node.send_message(inval_msg, node, time)
                self._config.report_op_send(node, inval_msg.operation, time)
            pending_req.expected_acks = len(write_nodes) + len(inval_nodes)
        elif op.op_type == kv.Operation.Type.DEL:
            for node in mapped_nodes.dest_nodes:
                self._node.send_message(msg, node, time)
                self._config.report_op_send(node, op, time)
            pending_req.expected_acks = len(mapped_nodes.dest_nodes)
        else:
            raise ValueError("Invalid operation type")

        self._pending_requests[self._next_req_id] = pending_req
        self._next_req_id += 1

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVReply):
            self._config.report_op_receive(message.src)
            request = self._pending_requests[message.req_id]
            if request.operation.op_type == kv.Operation.Type.GET:
                self._complete_request(message.req_id, message.result, time)
            else:
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
            # Only reply to client requests
            if message.type == MemcacheKVRequest.Type.CLIENT:
                reply = MemcacheKVReply(src = self._node,
                                        req_id = message.req_id,
                                        result = result,
                                        value = value)
                self._node.send_message(reply, message.src, time)

            if message.migration_dests is not None:
                assert message.operation.op_type == kv.Operation.Type.GET
                request = MemcacheKVRequest(type = MemcacheKVRequest.Type.MIGRATION,
                                            src = self._node,
                                            req_id = None,
                                            operation = kv.Operation(op_type = kv.Operation.Type.PUT,
                                                                     key = message.operation.key,
                                                                     value = value))
                for node in message.migration_dests:
                    self._node.send_message(request, node, time)
        else:
            raise ValueError("Invalid message type")


class MemcacheKVMigrationServer(kv.KV):
    """
    Implementation of a memcache style distributed key-value store server.
    Server actively migrates keys when overloaded.
    """
    def __init__(self, generator, stats):
        super().__init__(generator, stats)

    def _check_load_and_migrate(self, key, time):
        # Check if node is overloaded. If yes, migrate key value pair
        # to another node.
        total_iload = sum(self._config.iloads.values())
        expected_iload = (self._config.c * total_iload) / len(self._config.cache_nodes)
        total_pload = sum(self._config.ploads.values())
        expected_pload = (self._config.c * total_pload) / len(self._config.cache_nodes)

        if self._config.iloads[self._node.id] <= expected_iload:
            return None
        if self._config.ploads[self._node.id] <= expected_pload:
            return None

        for dest_node_id in sorted(self._config.ploads, key=self._config.ploads.get):
            if self._config.ploads[dest_node_id] > expected_pload:
                return None
            if self._config.iloads[dest_node_id] <= expected_iload:
                break

        assert dest_node_id != self._node.id
        request = MemcacheKVRequest(type = MemcacheKVRequest.Type.MIGRATION,
                                    src = self._node,
                                    req_id = None,
                                    operation = kv.Operation(op_type = kv.Operation.Type.PUT,
                                                             key = key,
                                                             value = self._store.get(key, "")))
        self._node.send_message(request, self._config.cache_nodes[dest_node_id], time)

    def _process_message(self, message, time):
        if isinstance(message, MemcacheKVRequest):
            # Always execute the request regardless
            # of node's load.
            result, value = self._execute_op(message.operation)

            if message.type == MemcacheKVRequest.Type.CLIENT:
                reply = MemcacheKVReply(src = self._node,
                                        req_id = message.req_id,
                                        result = result,
                                        value = value)
                self._node.send_message(reply, message.src, time)

                # Only do migration for GET and PUT requests.
                if message.operation.op_type == kv.Operation.Type.GET or \
                        message.operation.op_type == kv.Operation.Type.PUT:
                    self._check_load_and_migrate(message.operation.key, time)
            elif message.type == MemcacheKVRequest.Type.MIGRATION:
                # Notify config that migration is completed
                self._config.report_migration(message.operation.key,
                                              message.src,
                                              self._node)
            else:
                raise ValueError("Invalid MemcacheKVRequest type")
        else:
            raise ValueError("Invalid message type")
