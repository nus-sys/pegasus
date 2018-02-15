"""
memcachekv.py: Memcache style distributed key-value store.
"""

import pegasus.message
import pegasus.config
import pegasus.applications.kv as kv

class Request(pegasus.message.Message):
    """
    Request message used by MemcacheKV.
    """
    def __init__(self, src, src_time, operation):
        super().__init__(operation.len())
        self.src = src
        self.src_time = src_time
        self.operation = operation


class Reply(pegasus.message.Message):
    """
    Reply message used by MemcacheKV.
    """
    def __init__(self, src_time, op_type, result, value):
        super().__init__(kv.RES_LEN + len(value))
        self.src_time = src_time
        self.op_type = op_type
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
        msg = Request(src = self._node,
                      src_time = time,
                      operation = op)
        self._node.send_message(msg, dest_node, time)

    def _process_message(self, message, time):
        if isinstance(message, Request):
            result, value = self._execute_op(message.operation)
            reply = Reply(src_time = message.src_time,
                          op_type = message.operation.op_type,
                          result = result,
                          value = value)
            self._node.send_message(reply, message.src, time)
        elif isinstance(message, Reply):
            hit = True
            if (message.op_type == kv.Operation.Type.GET):
                if message.result == kv.Result.NOT_FOUND:
                    hit = False
            self._stats.report_op(message.op_type,
                                  time - message.src_time,
                                  hit)
        else:
            raise ValueError("Invalid message type")
