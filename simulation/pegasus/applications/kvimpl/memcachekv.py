"""
memcachekv.py: Memcache style distributed key-value store.
"""

import pegasus.message
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


class KeyNodeMap(object):
    """
    Abstract class. Defines the mapping from keys to nodes.
    Subclass of ``KeyNodeMap`` should implement ``key_to_node``.
    """
    def __init__(self, nodes):
        self._nodes = nodes

    def key_to_node(self, key):
        raise NotImplementedError


class MemcacheKV(kv.KV):
    """
    Implementation of a memcache style distributed key-value store.
    """
    def __init__(self, generator, key_node_map):
        super().__init__(generator)
        self._key_node_map = key_node_map
        self.cache_hits = 0
        self.cache_misses = 0
        self.received_replies = {}
        for op_type in kv.Operation.Type:
            self.received_replies[op_type] = 0

    def _execute(self, op, time):
        """
        Always send the operation to a remote node. Client nodes
        in a MemcacheKV are stateless, and do not store kv pairs.
        """
        dest_node = self._key_node_map.key_to_node(op.key)
        assert dest_node is not self._local_node
        msg = Request(src = self._local_node,
                      src_time = time,
                      operation = op)
        self._local_node.send_message(msg, dest_node, time)

    def _process_message(self, message, time):
        if isinstance(message, Request):
            ret = self._execute_op(message.operation)
            reply = Reply(src_time = message.src_time,
                          op_type = message.operation.op_type,
                          result = ret[0],
                          value = ret[1])
            self._local_node.send_message(reply, message.src, time)
        elif isinstance(message, Reply):
            self.received_replies[message.op_type] += 1
            if (message.op_type == kv.Operation.Type.GET):
                if message.result == kv.Result.OK:
                    self.cache_hits += 1
                else:
                    self.cache_misses += 1
        else:
            raise ValueError("Invalid message type")
