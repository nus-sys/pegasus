"""
test_pegasuskv.py: Unit tests for PegasusKV.
"""

import unittest
import pegasus.node
import pegasus.simulator
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.pegasuskv as pegasuskv
import pegasus.param as param

class TestConfig(pegasuskv.PegasusKVConfiguration):
    def __init__(self, cache_nodes, db_node, dir_node_id):
        super().__init__(cache_nodes, db_node)
        assert dir_node_id < len(self.cache_nodes)
        self.dir_node_id = dir_node_id
        self.next_cache_node = 0

    def select_cache_node(self, client_node, key):
        return self.cache_nodes[self.next_cache_node]

    def select_dir_node(self, client_node, key):
        return self.cache_nodes[self.dir_node_id]


class BasicTest(unittest.TestCase):
    N_CACHE_NODES = 4
    def setUp(self):
        # 1 client node and four cache nodes in the same rack
        rack = pegasus.node.Rack()
        self.cache_nodes = []
        self.server_apps = []
        self.stats = kv.KVStats()
        for i in range(self.N_CACHE_NODES):
            self.cache_nodes.append(pegasus.node.Node(parent = rack, id = i))
        self.client_node = pegasus.node.Node(parent = rack, id = self.N_CACHE_NODES)

        # Single directory config with cache node 0 as the directory
        self.config = TestConfig(cache_nodes = self.cache_nodes,
                                 db_node = None,
                                 dir_node_id = 0)
        # Register apps
        self.client_app = pegasuskv.PegasusKVClient(generator = None,
                                                    stats = self.stats)
        self.client_app.register_config(self.config)
        self.client_node.register_app(self.client_app)
        for node in self.cache_nodes:
            server_app = pegasuskv.PegasusKVServer(generator = None,
                                                   stats = self.stats)
            server_app.register_config(self.config)
            node.register_app(server_app)
            self.server_apps.append(server_app)

    def run_cache_nodes(self, time):
        for node in self.cache_nodes:
            node.run(time)

    def test_basic(self):
        timer = 0
        # PUT k1 v1
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertTrue(0 in self.server_apps[0]._directory['k1'].sharers)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)

        # GET k1 (node 0)
        self.config.next_cache_node = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_cache_nodes(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.stats.cache_hits, 1)

        # PUT k2 v2
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k2', 'v2'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_cache_nodes(timer)
        self.assertTrue(0 in self.server_apps[0]._directory['k2'].sharers)
        self.assertEqual(self.server_apps[0]._store['k2'], 'v2')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         2)

        # GET k1 (node 1)
        self.config.next_cache_node = 1
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertFalse(1 in self.server_apps[0]._directory['k1'].sharers)
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertTrue(1 in self.server_apps[0]._directory['k1'].sharers)
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         2)
        self.assertEqual(self.stats.cache_hits, 2)

        # GET k1 (node 2)
        self.config.next_cache_node = 2
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(4):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_cache_nodes(timer)
        self.assertTrue(2 in self.server_apps[0]._directory['k1'].sharers)
        self.assertEqual(self.server_apps[2]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         3)
        self.assertEqual(self.stats.cache_hits, 3)

        # PUT k1 vv1
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'vv1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.server_apps[0]._directory['k1'].sharers, {0, 1, 2})
        self.assertEqual(self.server_apps[0]._store['k1'], 'vv1')
        for i in range(1, 3):
            self.assertEqual(self.server_apps[i]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         2)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        for i in range(3):
            self.assertEqual(self.server_apps[i]._store['k1'], 'vv1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         2)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.server_apps[0]._directory['k1'].sharers, {0, 1, 2})
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         3)

        # DEL k1
        self.client_app._execute(kv.Operation(kv.Operation.Type.DEL, 'k1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(len(self.server_apps[0]._directory['k1'].sharers), 0)
        self.assertFalse('k1' in self.server_apps[0]._store)
        for i in range(1, 3):
            self.assertEqual(self.server_apps[i]._store['k1'], 'vv1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         0)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        for i in range(3):
            self.assertFalse('k1' in self.server_apps[i]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         0)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(len(self.server_apps[0]._directory['k1'].sharers), 0)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         1)

        # GET k2 (node 2)
        self.config.next_cache_node = 2
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         3)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(len(self.server_apps[0]._directory['k1'].sharers), 0)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         3)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertFalse('k1' in self.server_apps[2]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         3)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_cache_nodes(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         4)
        self.assertEqual(self.stats.cache_hits, 3)
        self.assertEqual(self.stats.cache_misses, 1)
