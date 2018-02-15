"""
memcachekv.py: Memcache style distributed key-value store.
"""

import pegasus.message
import pegasus.config
import pegasus.applications.kv as kv

class MemcacheKVRequest(pegasus.message.Message):
    """
    Request message used by MemcacheKV.
    """
    def __init__(self, src, req_id, operation):
        super().__init__(operation.len())
        self.src = src
        self.req_id = req_id
        self.operation = operation


class MemcacheKVReply(pegasus.message.Message):
    """
    Reply message used by MemcacheKV.
    """
    def __init__(self, req_id, result, value):
        super().__init__(kv.RES_LEN + len(value))
        self.req_id = req_id
        self.result = result
        self.value = value


class MemcacheKVConfiguration(pegasus.config.Configuration):
    """
    Abstract configuration class. Subclass of ``MemcacheKVConfiguration``
    should implement ``key_to_node``.
    """
    def __init__(self, cache_nodes, db_node):
        super().__init__()
        self.cache_nodes = cache_nodes
        self.db_node = db_node

    def key_to_node(self, key):
        """
        Return a node the ``key`` is mapped to.
        """
        raise NotImplementedError


class StaticConfig(MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node):
        super().__init__(cache_nodes, db_node)

    def key_to_node(self, key):
        return self.cache_nodes[hash(key) % len(self.cache_nodes)]


class MemcacheKV(kv.KV):
    """
    Implementation of a memcache style distributed key-value store.
    """
    def __init__(self, generator, stats):
        super().__init__(generator, stats)

    def _execute(self, op, time):
        """
        Always send the operation to a remote node. Client nodes
        in a MemcacheKV are stateless, and do not store kv pairs.
        """
        dest_node = self._config.key_to_node(op.key)
        assert dest_node is not self._node
        self._pending_requests[self._next_req_id] = kv.KV.PendingRequest(operation = op,
                                                                        time = time)
        msg = MemcacheKVRequest(src = self._node,
                                req_id = self._next_req_id,
                                operation = op)
        self._node.send_message(msg, dest_node, time)
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
            hit = True
            if request.operation.op_type == kv.Operation.Type.GET:
                if message.result == kv.Result.NOT_FOUND:
                    hit = False
            self._stats.report_op(request.operation.op_type,
                                  time - request.time,
                                  hit)
            del self._pending_requests[message.req_id]
        else:
            raise ValueError("Invalid message type")
