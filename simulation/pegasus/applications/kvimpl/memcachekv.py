"""
memcachekv.py: Memcache style distributed key-value store.
"""

import random

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


class MemcacheKV(kv.KV):
    """
    Implementation of a memcache style distributed key-value store.
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
        if isinstance(message, MemcacheKVRequest):
            result, value = self._execute_op(message.operation)
            reply = MemcacheKVReply(req_id = message.req_id,
                                    result = result,
                                    value = value)
            self._node.send_message(reply, message.src, time)
        elif isinstance(message, MemcacheKVReply):
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
