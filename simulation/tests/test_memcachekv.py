"""
test_memcachekv.py: Unit tests for MemcacheKV.
"""

import unittest
import pegasus.node
import pegasus.simulator
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv
import pegasus.param as param

class MemcacheKVSingleAppTest(unittest.TestCase):
    def setUp(self):
        self.kvapp = memcachekv.MemcacheKVServer(None,
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
        def __init__(self, cache_nodes, db_node, write_type):
            assert len(cache_nodes) == 1
            super().__init__(cache_nodes, db_node, write_type)

        def key_to_nodes(self, key, op_type):
            return memcachekv.MappedNodes([self.cache_nodes[0]], None)

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.server = pegasus.node.Node(rack, 1)
        self.stats = kv.KVStats()
        config = self.SingleServerConfig([self.server], None, memcachekv.WriteMode.UPDATE)
        self.client_app = memcachekv.MemcacheKVClient(None,
                                                      self.stats)
        self.client_app.register_config(config)
        self.server_app = memcachekv.MemcacheKVServer(None,
                                                      self.stats)
        self.server_app.register_config(config)
        self.client.register_app(self.client_app)
        self.server.register_app(self.server_app)

    def test_basic(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.server.run(timer)
        self.assertEqual(self.server_app._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         0)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.server.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
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
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
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


class MultiServerTest(unittest.TestCase):
    class MultiServerConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node, write_type):
            super().__init__(cache_nodes, db_node, write_type)
            self.next_nodes = []

        def key_to_nodes(self, key, op_type):
            return memcachekv.MappedNodes(self.next_nodes, None)

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.servers = []
        self.server_apps = []
        for i in range(2):
            self.servers.append(pegasus.node.Node(rack, i + 1))
        self.stats = kv.KVStats()
        self.config = self.MultiServerConfig(self.servers, None, memcachekv.WriteMode.UPDATE)
        self.client_app = memcachekv.MemcacheKVClient(None,
                                                      self.stats)
        self.client_app.register_config(self.config)
        self.client.register_app(self.client_app)
        for server in self.servers:
            app = memcachekv.MemcacheKVServer(None,
                                              self.stats)
            app.register_config(self.config)
            self.server_apps.append(app)
            server.register_app(app)

    def run_servers(self, end_time):
        for server in self.servers:
            server.run(end_time)

    def test_basic(self):
        timer = 0
        self.config.next_nodes = self.servers
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         0)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.run_servers(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.stats.cache_hits, 1)
        self.assertEqual(self.stats.cache_misses, 0)

        self.client_app._execute(kv.Operation(kv.Operation.Type.DEL, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertFalse('k1' in self.server_apps[0]._store)
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         1)
        self.assertEqual(self.stats.cache_hits, 1)
        self.assertEqual(self.stats.cache_misses, 0)

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         2)
        self.assertEqual(self.stats.cache_hits, 1)
        self.assertEqual(self.stats.cache_misses, 1)

    def test_write_modes(self):
        timer = 0
        # ANYNODE write mode
        self.config.write_mode = memcachekv.WriteMode.ANYNODE
        self.config.next_nodes = self.servers
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.run_servers(timer)
        if 'k1' in self.server_apps[0]._store:
            self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
            self.assertFalse('k1' in self.server_apps[1]._store)
            self.config.next_nodes = [self.servers[1]]
        if 'k1' in self.server_apps[1]._store:
            self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
            self.assertFalse('k1' in self.server_apps[0]._store)
            self.config.next_nodes = [self.servers[0]]
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         0)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client.run(timer)
        self.run_servers(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         2)
        self.config.next_nodes = self.servers
        self.client_app._execute(kv.Operation(kv.Operation.Type.DEL, 'k1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertFalse('k1' in self.server_apps[0]._store)
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         2)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.DEL],
                         1)
        # INVALIDATE write mode
        self.config.write_mode = memcachekv.WriteMode.INVALIDATE
        self.config.next_nodes = [self.servers[0]]
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.config.next_nodes = [self.servers[1]]
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         4)
        self.config.next_nodes = self.servers
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v2'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v2')
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         5)
        self.config.write_mode = memcachekv.WriteMode.UPDATE
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v3'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v3')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v3')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT],
                         6)
        self.config.write_mode = memcachekv.WriteMode.INVALIDATE
        self.config.next_nodes = [self.servers[1], self.servers[0]]
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v4'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[1]._store['k1'], 'v4')
        self.assertFalse('k1' in self.server_apps[0]._store)


class MigrationTest(unittest.TestCase):
    class TestConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node, write_mode):
            super().__init__(cache_nodes, db_node, write_mode)
            self.migration_requests = None
            self.key_node_map = {}

        def key_to_nodes(self, key, op_type):
            return memcachekv.MappedNodes([self.cache_nodes[self.key_node_map[key]]],
                                          self.migration_requests)

    def setUp(self):
        rack = pegasus.node.Rack()
        self.servers = []
        self.server_apps = []
        for i in range(4):
            self.servers.append(pegasus.node.Node(rack, i))
        self.client = pegasus.node.Node(rack, 4)
        self.stats = kv.KVStats()
        self.config = self.TestConfig(self.servers, None, memcachekv.WriteMode.UPDATE)
        self.client_app = memcachekv.MemcacheKVClient(None,
                                                      self.stats)
        self.client_app.register_config(self.config)
        self.client.register_app(self.client_app)
        for server in self.servers:
            app = memcachekv.MemcacheKVServer(None,
                                              self.stats)
            app.register_config(self.config)
            self.server_apps.append(app)
            server.register_app(app)

    def run_servers(self, end_time):
        for server in self.servers:
            server.run(end_time)

    def test_basic(self):
        self.server_apps[0]._store['k1'] = 'v1'
        self.server_apps[0]._store['k2'] = 'v2'
        self.server_apps[0]._store['k3'] = 'v3'
        self.server_apps[0]._store['k4'] = 'v4'
        self.config.key_node_map['k1'] = 0
        self.config.migration_requests = [memcachekv.MemcacheKVRequest.MigrationRequest(['k1', 'k2'],
                                                                                        self.servers[1]),
                                          memcachekv.MemcacheKVRequest.MigrationRequest(['k3'],
                                                                                        self.servers[2]),
                                          memcachekv.MemcacheKVRequest.MigrationRequest(['k4'],
                                                                                        self.servers[3])]
        self.assertFalse('k1' in self.server_apps[1]._store)
        self.assertFalse('k2' in self.server_apps[1]._store)
        self.assertFalse('k3' in self.server_apps[2]._store)
        self.assertFalse('k4' in self.server_apps[3]._store)

        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'), timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET],
                         1)
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k2'], 'v2')
        self.assertEqual(self.server_apps[2]._store['k3'], 'v3')
        self.assertEqual(self.server_apps[3]._store['k4'], 'v4')


class SimulatorTest(unittest.TestCase):
    class StaticConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node, write_type):
            super().__init__(cache_nodes, db_node, write_type)

        def key_to_nodes(self, key, op_type):
            index = sum(map(lambda x : ord(x), key)) % len(self.cache_nodes)
            return memcachekv.MappedNodes([self.cache_nodes[index]], None)

    class SimpleGenerator(kv.KVWorkloadGenerator):
        def __init__(self):
            self.ops = [(kv.Operation(kv.Operation.Type.PUT,
                                      "k1",
                                      "v1"),
                         0),
                        (kv.Operation(kv.Operation.Type.PUT,
                                      "k2",
                                      "v2"),
                         round(0.5*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k1"),
                         round(1.2*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k3"),
                         round(1.7*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.PUT,
                                      "k3",
                                      "v3"),
                         round(2.5*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k3"),
                         round(3*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k2"),
                         round(3.9*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.DEL,
                                      "k1"),
                         round(4.3*param.propg_delay())),
                        (kv.Operation(kv.Operation.Type.GET,
                                      "k1"),
                         round(5*param.propg_delay()))]

        def next_operation(self):
            if len(self.ops) > 0:
                return self.ops.pop(0)
            else:
                return None, None

    def setUp(self):
        self.stats = kv.KVStats()
        self.simulator = pegasus.simulator.Simulator(self.stats)
        rack = pegasus.node.Rack(0)
        # Single client node and 4 cache nodes all in one rack
        self.client_node = pegasus.node.Node(rack, 0)
        self.cache_nodes = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i+1))

        config = self.StaticConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE)
        # Register applications
        self.client_app = memcachekv.MemcacheKVClient(self.SimpleGenerator(),
                                                      self.stats)
        self.client_app.register_config(config)
        self.client_node.register_app(self.client_app)
        self.simulator.register_config(config)

        self.server_apps = []
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, self.stats)
            app.register_config(config)
            node.register_app(app)
            self.server_apps.append(app)

        self.simulator.add_node(self.client_node)
        self.simulator.add_nodes(self.cache_nodes)

    def test_basic(self):
        self.simulator.run((5+2)*param.MAX_PROPG_DELAY+len(self.SimpleGenerator().ops)*param.MAX_PKT_PROC_LTC)
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


class LoadBalanceTest(unittest.TestCase):
    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = memcachekv.LoadBalanceConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE, 100, 10)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)

    def test_noreplication(self):
        # k1:80, k2:60, k3:40, k4:30, k5:20, k6:5
        self.server_apps[0]._key_request_counter['k1'] = 30
        self.server_apps[0]._key_request_counter['k4'] = 20
        self.server_apps[0]._key_request_counter['k6'] = 5
        self.server_apps[1]._key_request_counter['k2'] = 60
        self.server_apps[1]._key_request_counter['k4'] = 10
        self.server_apps[1]._key_request_counter['k5'] = 10
        self.server_apps[2]._key_request_counter['k1'] = 50
        self.server_apps[2]._key_request_counter['k3'] = 40
        self.server_apps[2]._key_request_counter['k5'] = 10

        self.config.collect_load(1000000)
        self.config.rebalance_load()
        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4', 'k5', 'k6']:
            nodes = self.config.key_to_nodes(key, None)
            self.assertEqual(len(nodes.dst_nodes), 1)
            keys = node_to_keys.setdefault(nodes.dst_nodes[0].id, [])
            keys.append(key)
        for _, keys in node_to_keys.items():
            if 'k1' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k2' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k3' in keys:
                self.assertTrue('k6' in keys)
            elif 'k4' in keys:
                self.assertTrue('k5' in keys)

    def test_replication(self):
        # k1:220, k2:120, k3:40, k4:10
        self.server_apps[0]._key_request_counter['k1'] = 210
        self.server_apps[0]._key_request_counter['k2'] = 120
        self.server_apps[0]._key_request_counter['k3'] = 40
        self.server_apps[0]._key_request_counter['k4'] = 10
        self.config.collect_load(1000000)
        self.config.rebalance_load()

        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4']:
            nodes = self.config.key_to_nodes(key, None)
            if key == 'k1':
                self.assertEqual(len(nodes.dst_nodes), 3)
            elif key == 'k2':
                self.assertEqual(len(nodes.dst_nodes), 4)
            else:
                self.assertEqual(len(nodes.dst_nodes), 1)
            for node in nodes.dst_nodes:
                keys = node_to_keys.setdefault(node.id, [])
                keys.append(key)

        for _, keys in node_to_keys.items():
            if 'k1' in keys:
                self.assertEqual(len(keys), 2)
                self.assertTrue('k2' in keys)
            elif 'k3' in keys:
                self.assertEqual(len(keys), 3)
                self.assertTrue('k2' in keys)
                self.assertTrue('k4' in keys)

        # Rebalance
        self.server_apps[0]._key_request_counter['k1'] = 80
        self.server_apps[0]._key_request_counter['k2'] = 60
        self.server_apps[0]._key_request_counter['k3'] = 40
        self.server_apps[0]._key_request_counter['k4'] = 30
        self.server_apps[0]._key_request_counter['k5'] = 20
        self.server_apps[0]._key_request_counter['k6'] = 5
        self.config.collect_load(1000000)
        self.config.rebalance_load()
        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4', 'k5', 'k6']:
            nodes = self.config.key_to_nodes(key, None)
            self.assertEqual(len(nodes.dst_nodes), 1)
            keys = node_to_keys.setdefault(nodes.dst_nodes[0].id, [])
            keys.append(key)
        for _, keys in node_to_keys.items():
            if 'k1' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k2' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k3' in keys:
                self.assertTrue('k6' in keys)
            elif 'k4' in keys:
                self.assertTrue('k5' in keys)

    def test_nodereport(self):
        self.server_apps[0]._key_request_counter['k1'] = 50
        self.server_apps[0]._key_request_counter['k5'] = 20
        self.server_apps[1]._key_request_counter['k1'] = 30
        self.server_apps[1]._key_request_counter['k2'] = 30
        self.server_apps[1]._key_request_counter['k3'] = 40
        self.server_apps[2]._key_request_counter['k2'] = 30
        self.server_apps[2]._key_request_counter['k6'] = 5
        self.server_apps[2]._key_request_counter['k4'] = 30

        for node in self.cache_nodes:
            node.run(1000000)
        self.config.run(1000000)

        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4', 'k5', 'k6']:
            nodes = self.config.key_to_nodes(key, None)
            self.assertEqual(len(nodes.dst_nodes), 1)
            keys = node_to_keys.setdefault(nodes.dst_nodes[0].id, [])
            keys.append(key)
        for _, keys in node_to_keys.items():
            if 'k1' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k2' in keys:
                self.assertEqual(len(keys), 1)
            elif 'k3' in keys:
                self.assertTrue('k6' in keys)
            elif 'k4' in keys:
                self.assertTrue('k5' in keys)

        self.server_apps[0]._key_request_counter['k1'] = 90
        self.server_apps[0]._key_request_counter['k3'] = 10
        self.server_apps[1]._key_request_counter['k1'] = 90
        self.server_apps[1]._key_request_counter['k2'] = 10
        self.server_apps[2]._key_request_counter['k1'] = 30
        self.server_apps[2]._key_request_counter['k2'] = 70
        self.server_apps[3]._key_request_counter['k2'] = 40
        self.server_apps[3]._key_request_counter['k3'] = 30
        self.server_apps[3]._key_request_counter['k4'] = 10

        for node in self.cache_nodes:
            node.run(2000000)
        self.config.run(2000000)

        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4']:
            nodes = self.config.key_to_nodes(key, None)
            if key == 'k1':
                self.assertEqual(len(nodes.dst_nodes), 3)
            elif key == 'k2':
                self.assertEqual(len(nodes.dst_nodes), 4)
            else:
                self.assertEqual(len(nodes.dst_nodes), 1)
            for node in nodes.dst_nodes:
                keys = node_to_keys.setdefault(node.id, [])
                keys.append(key)

        for _, keys in node_to_keys.items():
            if 'k1' in keys:
                self.assertEqual(len(keys), 2)
                self.assertTrue('k2' in keys)
            elif 'k3' in keys:
                self.assertEqual(len(keys), 3)
                self.assertTrue('k2' in keys)
                self.assertTrue('k4' in keys)

class BoundedLoadTest(unittest.TestCase):
    class TestConfig(memcachekv.BoundedLoadConfig):
        def __init__(self, cache_nodes, db_node, write_type, c):
            super().__init__(cache_nodes, db_node, write_type, c)

        def key_hash(self, key):
            return sum(map(lambda x : ord(x), key))

    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = self.TestConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE, 1.5)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)
        self.client_node = pegasus.node.Node(rack, 4)
        self.stats = kv.KVStats()
        self.client_app = memcachekv.MemcacheKVClient(None, self.stats)
        self.client_app.register_config(self.config)
        self.client_node.register_app(self.client_app)

    def run_servers(self, end_time):
        for node in self.cache_nodes:
            node.run(end_time)

    def test_basic(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k2', 'v2'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k3', 'v3'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k4', 'v4'),
                                 timer)
        for node in self.cache_nodes:
            self.assertEqual(len(node._inflight_messages), 1)

        for _ in range(1):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                     timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 3)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 3)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)

        for _ in range(1):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                     timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 2)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 3)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 4)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 5)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 5)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 4)

        timer += param.MAX_PROPG_DELAY + 5 * param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        # Process migration messages
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.client_node.run(timer)

        for node in self.cache_nodes:
            self.assertEqual(len(node._inflight_messages), 0)

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 0)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)

    def test_migration(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertFalse('k1' in self.server_apps[1]._store)

        for _ in range(2):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                     timer)
        timer += param.MAX_PROPG_DELAY + 2 * param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_servers(timer)
        self.assertFalse('k1' in self.server_apps[1]._store)
        timer += param.MAX_PROPG_DELAY + 2 * param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 2)

        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v2'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v2')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 2)

        for _ in range(2):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                     timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + 2 * param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v2')
        self.assertEqual(self.server_apps[2]._store['k1'], 'v2')

        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v3'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v2')
        self.assertEqual(self.server_apps[2]._store['k1'], 'v3')

class MigrationServerTest(unittest.TestCase):
    class TestConfig(memcachekv.RoutingConfig):
        def __init__(self, cache_nodes, db_node, write_type, c):
            super().__init__(cache_nodes, db_node, write_type, c)

        def key_hash(self, key):
            return sum(map(lambda x : ord(x), key))

    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = self.TestConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE, 1.5)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVMigrationServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)
        self.client_node = pegasus.node.Node(rack, 4, logical_client=True)
        self.stats = kv.KVStats()
        self.client_app = memcachekv.MemcacheKVClient(None, self.stats)
        self.client_app.register_config(self.config)
        self.client_node.register_app(self.client_app)

    def run_servers(self, end_time):
        for node in self.cache_nodes:
            node.run(end_time)

    def test_basic(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)

        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.run_servers(timer)
            self.client_node.run(timer)

        # Migrate 'k1' from node 0 to node 2
        self.assertFalse('k1' in self.server_apps[2]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 0)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)
        self.assertEqual(len(self.cache_nodes[2]._inflight_messages), 0)
        self.config.iloads[0] = 2
        self.config.iloads[1] = 1
        self.config.iloads[2] = 1
        self.config.iloads[3] = 1
        self.config.ploads[0] = 3
        self.config.ploads[1] = 2
        self.config.ploads[2] = 0
        self.config.ploads[3] = 1

        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.assertEqual(len(self.cache_nodes[2]._inflight_messages), 1)

        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.client_node.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 1)
        self.assertEqual(self.server_apps[2]._store['k1'], 'v1')
        self.assertEqual(self.config.key_node_map['k1'], 2)

        # Migrate 'k1' from node 2 to node 3
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 1)
        self.assertFalse('k1' in self.server_apps[3]._store)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v2'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 0)
        self.assertEqual(len(self.cache_nodes[2]._inflight_messages), 1)
        self.config.iloads[0] = 2
        self.config.iloads[1] = 1
        self.config.iloads[2] = 4
        self.config.iloads[3] = 3
        self.config.ploads[0] = 3
        self.config.ploads[1] = 3
        self.config.ploads[2] = 5
        self.config.ploads[3] = 1

        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 1)

        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.client_node.run(timer)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 2)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[2]._store['k1'], 'v2')
        self.assertEqual(self.server_apps[3]._store['k1'], 'v2')
        self.assertEqual(self.config.key_node_map['k1'], 3)


class DynamicCHTest(unittest.TestCase):
    class TestConfig(memcachekv.DynamicCHConfig):
        def __init__(self, cache_nodes, db_node, write_mode, c, hash_space):
            super().__init__(cache_nodes, db_node, write_mode, c, hash_space)

        def key_hash_fn(self, key):
            return sum(map(lambda x : ord(x), key))

    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = self.TestConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE, 1.5, 16)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)
        self.client_node = pegasus.node.Node(rack, 4)
        self.stats = kv.KVStats()
        self.client_app = memcachekv.MemcacheKVClient(None, self.stats)
        self.client_app.register_config(self.config)
        self.client_node.register_app(self.client_app)

    def run_servers(self, end_time):
        for node in self.cache_nodes:
            node.run(end_time)

    def test_basic(self):
        self.config.c = 100 # prevent migration
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k2', 'v2'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k6', 'v6'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'kk', 'vk'),
                                 timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_servers(timer)
        timer += param.MAX_PROPG_DELAY + 4 * param.MAX_PKT_PROC_LTC
        self.client_node.run(timer)
        self.run_servers(timer)
        self.assertEqual(self.server_apps[3]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[0]._store['k2'], 'v2')
        self.assertEqual(self.server_apps[1]._store['k6'], 'v6')
        self.assertEqual(self.server_apps[2]._store['kk'], 'vk')
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 4)

    def test_basic_migration(self):
        timer = 5
        self.server_apps[1]._store['k6'] = 'v6'
        self.server_apps[1]._store['k7'] = 'v7'
        self.server_apps[1]._store['k8'] = 'v8'
        self.server_apps[2]._store['kk'] = 'vk'
        self.config.install_key('k6')
        self.config.key_rates['k6'] = memcachekv.KeyRate(4, 1000000)
        self.config.install_key('k7')
        self.config.key_rates['k7'] = memcachekv.KeyRate(2, 1000000)
        self.config.install_key('k8')
        self.config.key_rates['k8'] = memcachekv.KeyRate(2, 1000000)
        self.config.install_key('kk')
        self.config.key_rates['kk'] = memcachekv.KeyRate(4, 1000000)

        self.config.ploads[1] = 8
        self.config.iloads[1] = 3
        self.config.ploads[2] = 4
        self.config.iloads[2] = 1

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k6'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)

        self.assertEqual(self.server_apps[2]._store['k7'], 'v7')
        self.assertEqual(self.server_apps[2]._store['k8'], 'v8')
        self.assertFalse('k6' in self.server_apps[2]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 1)

        self.config.c = 100 # prevent migration
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k6', 'V6'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k7', 'V7'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k8', 'V8'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'kk', 'Vk'),
                                 timer)

        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + 3*param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[1]._store['k6'], 'V6')
        self.assertEqual(self.server_apps[1]._store['k7'], 'v7')
        self.assertEqual(self.server_apps[1]._store['k8'], 'v8')
        self.assertEqual(self.server_apps[2]._store['k7'], 'V7')
        self.assertEqual(self.server_apps[2]._store['k8'], 'V8')
        self.assertEqual(self.server_apps[2]._store['kk'], 'Vk')
        self.assertFalse('k6' in self.server_apps[2]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 4)

    def test_rounding_migration(self):
        timer = 5
        self.server_apps[0]._store['k5'] = 'v5'
        self.server_apps[0]._store['k4'] = 'v4'
        self.server_apps[0]._store['k2'] = 'v2'
        self.server_apps[3]._store['k1'] = 'v1'
        self.config.install_key('k5')
        self.config.key_rates['k5'] = memcachekv.KeyRate(2, 1000000)
        self.config.install_key('k4')
        self.config.key_rates['k4'] = memcachekv.KeyRate(2, 1000000)
        self.config.install_key('k2')
        self.config.key_rates['k2'] = memcachekv.KeyRate(4, 1000000)
        self.config.install_key('k1')
        self.config.key_rates['k1'] = memcachekv.KeyRate(4, 1000000)

        self.config.ploads[0] = 8
        self.config.iloads[0] = 3
        self.config.ploads[3] = 4
        self.config.iloads[3] = 1

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k5'),
                                 timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)

        self.assertEqual(self.server_apps[1]._store['k5'], 'v5')
        self.assertEqual(self.server_apps[1]._store['k4'], 'v4')
        self.assertFalse('k2' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 1)

        self.config.c = 100 # prevent migration
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k5', 'V5'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k4', 'V4'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k2', 'V2'),
                                 timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'V1'),
                                 timer)

        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + 3*param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k2'], 'V2')
        self.assertEqual(self.server_apps[0]._store['k5'], 'v5')
        self.assertEqual(self.server_apps[0]._store['k4'], 'v4')
        self.assertEqual(self.server_apps[1]._store['k5'], 'V5')
        self.assertEqual(self.server_apps[1]._store['k4'], 'V4')
        self.assertEqual(self.server_apps[3]._store['k1'], 'V1')
        self.assertFalse('k2' in self.server_apps[1]._store)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.GET], 1)
        self.assertEqual(self.stats.received_replies[kv.Operation.Type.PUT], 4)

class CoTTest(unittest.TestCase):
    class TestConfig(memcachekv.CoTConfig):
        def __init__(self, cache_nodes, db_node, write_mode):
            super().__init__(cache_nodes, db_node, write_mode)

        def key_hash_fn_a(self, key):
            return sum(map(lambda x : ord(x), key))

        def key_hash_fn_b(self, key):
            return sum(map(lambda x : ord(x), key)) + 1

    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = self.TestConfig(self.cache_nodes, None, memcachekv.WriteMode.UPDATE)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)
        self.client_node = pegasus.node.Node(rack, 4)
        self.stats = kv.KVStats()
        self.client_app = memcachekv.MemcacheKVClient(None, self.stats)
        self.client_app.register_config(self.config)
        self.client_node.register_app(self.client_app)

    def run_servers(self, end_time):
        for node in self.cache_nodes:
            node.run(end_time)

    def test_basic(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'), timer)
        for _ in range(2):
            timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
            self.client_node.run(timer)
            self.run_servers(timer)
        self.assertEqual(self.server_apps[0]._store['k1'], 'v1')
        self.assertEqual(self.server_apps[1]._store['k1'], 'v1')

        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'), timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 0)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'), timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'), timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'), timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
