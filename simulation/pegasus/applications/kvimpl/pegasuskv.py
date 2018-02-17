"""
pegasuskv.py: Distributed key-value store using Pegasus
"""

import random
import enum

import pegasus.message
import pegasus.config
import pegasus.applications.kv as kv

class PegasusKVRequest(pegasus.message.Message):
    class RequestType(enum.Enum):
        CACHE = 1,
        DIRECTORY = 2,
        FORWARD = 3

    def __init__(self, src, req_id, req_type, operation):
        super().__init__(kv.REQ_ID_LEN + operation.len())
        self.src = src
        self.req_id = req_id
        self.req_type = req_type
        self.operation = operation


class PegasusKVReply(pegasus.message.Message):
    def __init__(self, req_id, result, value):
        super().__init__(kv.REQ_ID_LEN + kv.RES_LEN + len(value))
        self.req_id = req_id
        self.result = result
        self.value = value


class DirectoryReply(pegasus.message.Message):
    """
    Directory reply for PUT and DEL operations. Contains the
    number of expected acks.
    """
    def __init__(self, req_id, num_acks):
        super().__init__(kv.REQ_ID_LEN)
        self.req_id = req_id
        self.num_acks = num_acks


class PegasusKVConfiguration(pegasus.config.Configuration):
    """
    Abstract configuration class. Subclass of ``PegasusKVConfiguration``
    should implement ``client_to_cache_node`` and ``key_to_dir_node``.
    """
    def __init__(self, cache_nodes, db_node):
        super().__init__()
        self.cache_nodes = cache_nodes
        self.db_node = db_node

    def select_cache_node(self, client_node, key):
        """
        Return a cache node based on ``client_node`` and/or ``key``.
        """
        raise NotImplementedError

    def select_dir_node(self, client_node, key):
        """
        Return a directoy node based on ``client_node`` and/or ``key``.
        """
        raise NotImplementedError


class SingleDirectoryConfig(PegasusKVConfiguration):
    """
    Pegasus configuration with a single directory node (co-locate with
    one of the cache nodes). Clients send requests to randomly selected
    cache nodes.
    """
    def __init__(self, cache_nodes, db_node, dir_node_id):
        super().__init__(cache_nodes, db_node)
        assert dir_node_id < len(self.cache_nodes)
        self.dir_node_id = dir_node_id

    def select_cache_node(self, client_node, key):
        return random.choice(self.cache_nodes)

    def select_dir_node(self, client_node, key):
        return self.cache_nodes[self.dir_node_id]


class PegasusKVClient(kv.KV):
    """
    Implementation of distributed key-value store client using Pegasus.
    """
    class PendingRequest(kv.KV.PendingRequest):
        def __init__(self, operation, time):
            super().__init__(operation, time)
            self.received_acks = 0
            self.expected_acks = -1

    def __init__(self, generator, stats):
        super().__init__(generator, stats)

    def _execute(self, op, time):
        """
        Send GET requests to a cache node determined by the configuration. For PUT
        and DEL requests, send them directly to directory nodes.
        """
        self._pending_requests[self._next_req_id] = self.PendingRequest(operation = op, time = time)
        if op.op_type == kv.Operation.Type.GET:
            dest_node = self._config.select_cache_node(self._node, op.key)
            req_type = PegasusKVRequest.RequestType.CACHE
        elif op.op_type == kv.Operation.Type.PUT or \
            op.op_type == kv.Operation.Type.DEL:
            dest_node = self._config.select_dir_node(self._node, op.key)
            req_type = PegasusKVRequest.RequestType.DIRECTORY
        else:
            raise ValueError("Invalid operation type")

        msg = PegasusKVRequest(src = self._node,
                               req_id = self._next_req_id,
                               req_type = req_type,
                               operation = op)
        self._node.send_message(msg, dest_node, time)
        self._next_req_id += 1

    def _process_message(self, message, time):
        """
        Client should only receive two types of messages: PegasusKVReply and DirectoryReply.
        """
        if isinstance(message, PegasusKVReply):
            self._process_cache_reply(message, time)
        elif isinstance(message, DirectoryReply):
            self._process_dir_reply(message, time)
        else:
            raise ValueError("Invalid message type")

    def _process_cache_reply(self, message, time):
        request = self._pending_requests[message.req_id]
        if request.operation.op_type == kv.Operation.Type.GET:
            self._complete_request(message.req_id, message.result, time)
        elif request.operation.op_type == kv.Operation.Type.PUT or \
            request.operation.op_type == kv.Operation.Type.DEL:
            request.received_acks += 1
            if request.expected_acks > -1 and request.received_acks >= request.expected_acks:
                self._complete_request(message.req_id, kv.Result.OK, time)
        else:
            raise ValueError("Invalid operation type")

    def _complete_request(self, req_id, result, time):
        request = self._pending_requests.pop(req_id)
        self._stats.report_op(request.operation.op_type,
                              time - request.time,
                              result == kv.Result.OK)


    def _process_dir_reply(self, message, time):
        request = self._pending_requests[message.req_id]
        request.expected_acks = message.num_acks
        if request.received_acks >= request.expected_acks:
            self._complete_request(message.req_id, kv.Result.OK, time)


class PegasusKVServer(kv.KV):
    """
    Implementation of distributed key-value store server using Pegasus.
    """
    class DirectoryEntry(object):
        """
        Directory entry for a single key. The sharers list stores
        the indices of sharer cache nodes.
        """
        def __init__(self):
            self.sharers = set()

    class PendingGETRequest(object):
        """
        Pending GET request that misses locally.
        """
        def __init__(self, client_node, key):
            self.client_node = client_node
            self.key = key

    def __init__(self, generator, stats):
        super().__init__(generator, stats)
        self._directory = {} # key -> DirectoryEntry
        self._pending_get_reqs = {} # req-id -> PendingGETRequest

    def _execute(self, op, time):
        # KV server should never generate requests
        raise NotImplementedError

    def _process_message(self, message, time):
        """
        Server should never receive DirectoryReply (directory directly sends
        DirectoryReply to clients).
        """
        if isinstance(message, PegasusKVRequest):
            if message.req_type == PegasusKVRequest.RequestType.CACHE:
                self._process_cache_request(message, time)
            elif message.req_type == PegasusKVRequest.RequestType.DIRECTORY:
                # Directory request
                self._process_dir_request(message, time)
            elif message.req_type == PegasusKVRequest.RequestType.FORWARD:
                # Forwarded request from directory
                self._process_forward_request(message, time)
            else:
                raise ValueError("Invalid PegasusRequest type")
        elif isinstance(message, PegasusKVReply):
            self._process_cache_reply(message, time)
        else:
            raise ValueError("Invalid message type")

    def _send_message(self, message, node, send_time):
        """
        A wrapper around node.send_message(). Deals with sending
        messages to itself.
        """
        if node is self._node:
            if isinstance(message, PegasusKVRequest):
                if message.req_type == PegasusKVRequest.RequestType.DIRECTORY:
                    self._process_dir_request(message, send_time)
                elif message.req_type == PegasusKVRequest.RequestType.FORWARD:
                    self._process_forward_request(message, send_time)
                else:
                    raise ValueError("Invalid request type")
            elif isinstance(message, PegasusKVReply):
                self._process_cache_reply(message, send_time)
            else:
                raise ValueError("Invalid message type")
        else:
            self._node.send_message(message, node, send_time)

    def _process_cache_request(self, message, time):
        """
        Process a single cache request. Cache request can only be
        GET. Clients send PUT and DEL requests as directory requests
        to the directory, and the directory forwards them to cache
        nodes as forwarded requests.
        """
        assert message.operation.op_type == kv.Operation.Type.GET
        # Try GET request locally. If hit, directly reply back to client.
        result, value = self._execute_op(message.operation)
        if result == kv.Result.OK:
            reply = PegasusKVReply(req_id = message.req_id,
                                   result = result,
                                   value = value)
            self._send_message(reply, message.src, time)
        elif result == kv.Result.NOT_FOUND:
            # On a cache miss, forward the request to the directory.
            message.req_type = PegasusKVRequest.RequestType.DIRECTORY
            self._pending_get_reqs[message.req_id] = self.PendingGETRequest(client_node = message.src,
                                                                            key = message.operation.key)
            message.src = self._node
            dir_node = self._config.select_dir_node(self._node, message.operation.key)
            self._send_message(message, dir_node, time)
        else:
            raise ValueError("Invalid reply result")

    def _process_dir_request(self, message, time):
        """
        Process a single directory request.
        """
        dir_entry = self._directory.setdefault(message.operation.key,
                                               self.DirectoryEntry())
        if message.operation.op_type == kv.Operation.Type.GET:
            if len(dir_entry.sharers) > 0:
                # Forward the GET request to a randomly selected sharer
                # (if local node is one of the sharers, always pick the
                # local node). Also add the requestor to the sharers list.
                message.req_type = PegasusKVRequest.RequestType.FORWARD
                if self._node.id in dir_entry.sharers:
                    sharer = self._node
                else:
                    sharer = self._config.cache_nodes[random.choice(dir_entry.sharers)]
                self._send_message(message, sharer, time)
                dir_entry.sharers.add(message.src.id)
            else:
                # Key not cached on any cache node, reply NOT_FOUND
                reply = PegasusKVReply(req_id = message.req_id,
                                       result = kv.Result.NOT_FOUND,
                                       value = "")
                self._send_message(reply, message.src, time)
        elif message.operation.op_type == kv.Operation.Type.PUT:
            # If there exists sharers, forward PUT to all sharers, and
            # reply to src with the number of expected acks. Otherwise,
            # "forward" PUT to the local cache node, and add it to the
            # sharers list.
            message.req_type = PegasusKVRequest.RequestType.FORWARD
            if len(dir_entry.sharers) > 0:
                for sharer in dir_entry.sharers:
                    self._send_message(message, self._config.cache_nodes[sharer], time)
            else:
                self._send_message(message, self._node, time)
                dir_entry.sharers.add(self._node.id)
            reply = DirectoryReply(message.req_id, len(dir_entry.sharers))
            self._send_message(reply, message.src, time)
        elif message.operation.op_type == kv.Operation.Type.DEL:
            # Forward DEL to all sharers (if exist), and clear the sharers list
            message.req_type = PegasusKVRequest.RequestType.FORWARD
            for sharer in dir_entry.sharers:
                self._send_message(message, self._config.cache_nodes[sharer], time)
            reply = DirectoryReply(message.req_id, len(dir_entry.sharers))
            self._send_message(reply, message.src, time)
            dir_entry.sharers.clear()
        else:
            raise ValueError("Invalid op type")

    def _process_forward_request(self, message, time):
        result, value = self._execute_op(message.operation)
        reply = PegasusKVReply(req_id = message.req_id,
                               result = result,
                               value = value)
        self._send_message(reply, message.src, time)

    def _process_cache_reply(self, message, time):
        """
        Cache nodes can only receive GET replies. On a GET hit reply,
        install the key-value pair locally.
        """
        request = self._pending_get_reqs.pop(message.req_id)
        if message.result == kv.Result.OK:
            self._store[request.key] = message.value
        # Forward reply to client
        self._send_message(message, request.client_node, time)
