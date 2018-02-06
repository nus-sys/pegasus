"""
test_memcachekv.py: Unit tests for MemcacheKV.
"""

import unittest
import pegasus.node
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv
from pegasus.config import *

class MemcacheKVSingleAppTest(unittest.TestCase):
    def setUp(self):
        self.kvapp = memcachekv.MemcacheKV(None, memcachekv.KeyNodeMap(None))

    def test_basic(self):
        ret = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'))
        self.assertEqual(ret[0], kv.Result.OK)
        self.assertEqual(len(ret[1]), 0)
        ret = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k1'))
        self.assertEqual(ret[0], kv.Result.OK)
        self.assertEqual(ret[1], 'v1')
        ret = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k2'))
        self.assertEqual(ret[0], kv.Result.NOT_FOUND)
        self.assertEqual(len(ret[1]), 0)
        ret = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.DEL, 'k1'))
        self.assertEqual(ret[0], kv.Result.OK)
        self.assertEqual(len(ret[1]), 0)
        ret = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k1'))
        self.assertEqual(ret[0], kv.Result.NOT_FOUND)
        self.assertEqual(len(ret[1]), 0)


class ClientServerTest(unittest.TestCase):
    class SingleServerMap(memcachekv.KeyNodeMap):
        def __init__(self, nodes):
            assert len(nodes) == 1
            super().__init__(nodes)

        def key_to_node(self, key):
            return self._nodes[0]

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.server = pegasus.node.Node(rack, 1)
        self.client_app = memcachekv.MemcacheKV(None,
                                                self.SingleServerMap([self.server]))
        self.server_app = memcachekv.MemcacheKV(None, None)
        self.client_app.register_nodes(self.client, [self.client, self.server])
        self.server_app.register_nodes(self.server, [self.client, self.server])
        self.client.register_app(self.client_app)
        self.server.register_app(self.server_app)

    def test_basic(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        timer += MIN_PROPG_DELAY + PKT_PROC_LTC
        self.client.run(timer)
        self.server.run(timer)
        self.assertEqual(self.server_app._store['k1'], 'v1')
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.PUT],
                         0)
        timer += MIN_PROPG_DELAY + PKT_PROC_LTC
        self.client.run(timer)
        self.server.run(timer)
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.PUT],
                         1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += MIN_PROPG_DELAY + PKT_PROC_LTC
            self.client.run(timer)
            self.server.run(timer)
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.client_app.cache_hits, 1)
        self.assertEqual(self.client_app.cache_misses, 0)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k2'),
                                 timer)
        for _ in range(2):
            timer += MIN_PROPG_DELAY + PKT_PROC_LTC
            self.client.run(timer)
            self.server.run(timer)
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.client_app.received_replies[kv.Operation.Type.GET],
                         2)
        self.assertEqual(self.client_app.cache_hits, 1)
        self.assertEqual(self.client_app.cache_misses, 1)
        self.assertEqual(len(self.client_app._store), 0)
        self.assertEqual(len(self.server_app._store), 1)
