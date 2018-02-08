"""
test_memcachekv.py: Unit tests for MemcacheKV.
"""

import unittest
import pegasus.node
import pegasus.simulator
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv
from pegasus.param import *

class MemcacheKVSingleAppTest(unittest.TestCase):
    def setUp(self):
        self.kvapp = memcachekv.MemcacheKV(None,
                                           kv.KVStats())

    def test_basic(self):
        result, value = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'))
        self.assertEqual(result, kv.Result.OK)
        self.assertEqual(len(value), 0)
        result, value = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k1'))
        self.assertEqual(result, kv.Result.OK)
        self.assertEqual(value, 'v1')
        result, value = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k2'))
        self.assertEqual(result, kv.Result.NOT_FOUND)
        self.assertEqual(len(value), 0)
        result, value = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.DEL, 'k1'))
        self.assertEqual(result, kv.Result.OK)
        self.assertEqual(len(value), 0)
        result, value = self.kvapp._execute_op(kv.Operation(kv.Operation.Type.GET, 'k1'))
        self.assertEqual(result, kv.Result.NOT_FOUND)
        self.assertEqual(len(value), 0)


class ClientServerTest(unittest.TestCase):
    class SingleServerConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node):
            assert len(cache_nodes) == 1
            super().__init__(cache_nodes, db_node)

        def key_to_node(self, key):
            return self.cache_nodes[0]

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.server = pegasus.node.Node(rack, 1)
        self.stats = kv.KVStats()
        self.client_app = memcachekv.MemcacheKV(None,
                                                self.stats)
        self.client_app.register_config(self.SingleServerConfig([self.server], None))
        self.server_app = memcachekv.MemcacheKV(None,
                                                self.stats)
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
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         0)
        timer += MIN_PROPG_DELAY + PKT_PROC_LTC
        self.client.run(timer)
        self.server.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += MIN_PROPG_DELAY + PKT_PROC_LTC
            self.client.run(timer)
            self.server.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.stats.cache_hits, 1)
        self.assertEqual(self.stats.cache_misses, 0)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k2'),
                                 timer)
        for _ in range(2):
            timer += MIN_PROPG_DELAY + PKT_PROC_LTC
            self.client.run(timer)
            self.server.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         2)
        self.assertEqual(self.stats.cache_hits, 1)
        self.assertEqual(self.stats.cache_misses, 1)
        self.assertEqual(len(self.client_app._store), 0)
        self.assertEqual(len(self.server_app._store), 1)


class SimulatorTest(unittest.TestCase):
    class StaticConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node):
            super().__init__(cache_nodes, db_node)

        def key_to_node(self, key):
            index = sum(map(lambda x : ord(x), key)) % len(self.cache_nodes)
            return self.cache_nodes[index]

    class SimpleGenerator(kv.KVWorkloadGenerator):
        def __init__(self):
            self.ops = [(kv.Operation(kv.Operation.Type.PUT,
                                      "k1",
                                      "v1"), 0),
                        (kv.Operation(kv.Operation.Type.PUT,
                                      "k2",
                                      "v2"), 15),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k1"), 23),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k3"), 40),
                        (kv.Operation(kv.Operation.Type.PUT,
                                      "k3",
                                      "v3"), 48),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k3"), 60),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k2"), 71),
                        (kv.Operation(kv.Operation.Type.DEL,
                                      "k1"), 75),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k1"), 88)]

        def next_operation(self):
            if len(self.ops) > 0:
                return self.ops.pop(0)
            else:
                return None, None

    def setUp(self):
        self.stats = kv.KVStats()
        self.simulator = pegasus.simulator.Simulator(self.stats)
        # Single client node in a dedicated rack
        self.client_node = pegasus.node.Node(pegasus.node.Rack(0), 0)
        # 4 cache nodes in one rack
        self.cache_nodes = []
        cache_rack = pegasus.node.Rack(1)
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(cache_rack, i))

        config = self.StaticConfig(self.cache_nodes, None)
        # Register applications
        self.client_app = memcachekv.MemcacheKV(self.SimpleGenerator(),
                                                self.stats)
        self.client_app.register_config(config)
        self.client_node.register_app(self.client_app)

        self.server_apps = []
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKV(None, self.stats)
            app.register_config(config)
            node.register_app(app)
            self.server_apps.append(app)

        self.simulator.add_node(self.client_node)
        self.simulator.add_nodes(self.cache_nodes)

    def test_basic(self):
        self.simulator.run(88 + 6 * MIN_PROPG_DELAY)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         5)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         3)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         1)
        self.assertEqual(self.stats.cache_hits, 3)
        self.assertEqual(self.stats.cache_misses, 2)
        self.assertEqual(len(self.server_apps[0]._store), 0)
        self.assertEqual(len(self.server_apps[1]._store), 1)
        self.assertTrue(self.server_apps[1]._store["k2"], "v2")
        self.assertEqual(len(self.server_apps[2]._store), 1)
        self.assertTrue(self.server_apps[2]._store["k3"], "v3")
        self.assertEqual(len(self.server_apps[3]._store), 0)
