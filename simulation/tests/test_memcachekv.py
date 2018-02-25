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
        def __init__(self, cache_nodes, db_node):
            assert len(cache_nodes) == 1
            super().__init__(cache_nodes, db_node)

        def key_to_nodes(self, key):
            return [self.cache_nodes[0]]

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.server = pegasus.node.Node(rack, 1)
        self.stats = kv.KVStats()
        config = self.SingleServerConfig([self.server], None)
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
        def __init__(self, cache_nodes, db_node):
            super().__init__(cache_nodes, db_node)
            self.next_nodes = []

        def key_to_nodes(self, key):
            return self.next_nodes

    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.servers = []
        self.server_apps = []
        for i in range(2):
            self.servers.append(pegasus.node.Node(rack, i + 1))
        self.stats = kv.KVStats()
        self.config = self.MultiServerConfig(self.servers, None)
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


class SimulatorTest(unittest.TestCase):
    class StaticConfig(memcachekv.MemcacheKVConfiguration):
        def __init__(self, cache_nodes, db_node):
            super().__init__(cache_nodes, db_node)

        def key_to_nodes(self, key):
            index = sum(map(lambda x : ord(x), key)) % len(self.cache_nodes)
            return [self.cache_nodes[index]]

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

        config = self.StaticConfig(self.cache_nodes, None)
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
        self.config = memcachekv.LoadBalanceConfig(self.cache_nodes, None, 100, 10)
        for node in self.cache_nodes:
            app = memcachekv.MemcacheKVServer(None, None)
            app.register_config(self.config)
            node.register_app(app)
            self.server_apps.append(app)

    def test_noreplication(self):
        # k1:80, k2:60, k3:40, k4:30, k5:20, k6:5
        report = memcachekv.LoadBalanceConfig.LoadReport()
        report.key_request_rates = [memcachekv.LoadBalanceConfig.KeyRequestRate('k1', 30),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k4', 20),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k6', 5)]
        self.config.report_load(None, report)
        report.key_request_rates = [memcachekv.LoadBalanceConfig.KeyRequestRate('k2', 60),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k4', 10),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k5', 10)]
        self.config.report_load(None, report)
        report.key_request_rates = [memcachekv.LoadBalanceConfig.KeyRequestRate('k1', 50),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k3', 40),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k5', 10)]
        self.config.report_load(None, report)

        self.config.rebalance_load()
        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4', 'k5', 'k6']:
            nodes = self.config.key_to_nodes(key)
            self.assertEqual(len(nodes), 1)
            keys = node_to_keys.setdefault(nodes[0].id, [])
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
        report = memcachekv.LoadBalanceConfig.LoadReport()
        report.key_request_rates = [memcachekv.LoadBalanceConfig.KeyRequestRate('k1', 210),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k2', 120),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k3', 40),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k4', 10)]
        self.config.report_load(None, report)
        self.config.rebalance_load()

        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4']:
            nodes = self.config.key_to_nodes(key)
            if key == 'k1':
                self.assertEqual(len(nodes), 3)
            elif key == 'k2':
                self.assertEqual(len(nodes), 4)
            else:
                self.assertEqual(len(nodes), 1)
            for node in nodes:
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
        report = memcachekv.LoadBalanceConfig.LoadReport()
        report.key_request_rates = [memcachekv.LoadBalanceConfig.KeyRequestRate('k1', 80),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k2', 60),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k3', 40),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k4', 30),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k5', 20),
                                    memcachekv.LoadBalanceConfig.KeyRequestRate('k6', 5)]
        self.config.report_load(None, report)

        self.config.rebalance_load()
        node_to_keys = {}
        for key in ['k1', 'k2', 'k3', 'k4', 'k5', 'k6']:
            nodes = self.config.key_to_nodes(key)
            self.assertEqual(len(nodes), 1)
            keys = node_to_keys.setdefault(nodes[0].id, [])
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
            nodes = self.config.key_to_nodes(key)
            self.assertEqual(len(nodes), 1)
            keys = node_to_keys.setdefault(nodes[0].id, [])
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
            nodes = self.config.key_to_nodes(key)
            if key == 'k1':
                self.assertEqual(len(nodes), 3)
            elif key == 'k2':
                self.assertEqual(len(nodes), 4)
            else:
                self.assertEqual(len(nodes), 1)
            for node in nodes:
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

class ConsistentHashingWithBoundedLoadTest(unittest.TestCase):
    class TestConfig(memcachekv.ConsistentHashingWithBoundedLoadConfig):
        def __init__(self, cache_nodes, db_node, c):
            super().__init__(cache_nodes, db_node, c)

        def key_hash(self, key):
            return sum(map(lambda x : ord(x), key))

    def setUp(self):
        rack = pegasus.node.Rack(0)
        self.cache_nodes = []
        self.server_apps = []
        for i in range(4):
            self.cache_nodes.append(pegasus.node.Node(rack, i))
        self.config = self.TestConfig(self.cache_nodes, None, 2)
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

        for _ in range(3):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 4)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 4)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
        for _ in range(7):
            self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                     timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 8)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 8)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 5)
        timer += param.MAX_PROPG_DELAY + 16 * param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        for node in self.cache_nodes:
            self.assertEqual(len(node._inflight_messages), 0)
        timer += param.MAX_PROPG_DELAY
        self.client_node.run(timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k4'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[3]._inflight_messages), 1)

    def test_queued_messages(self):
        timer = 0
        self.client_app._execute(kv.Operation(kv.Operation.Type.PUT, 'k1', 'v1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)
        timer += (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY) + param.MAX_PKT_PROC_LTC + 1
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 1)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        timer += (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY) + param.MAX_PKT_PROC_LTC + 1
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 1)
        timer += (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY) + param.MAX_PKT_PROC_LTC + 1
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
        timer += (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY) + param.MAX_PKT_PROC_LTC + 1
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 3)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
        timer = param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.run_servers(timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 2)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
        timer += param.MAX_PROPG_DELAY
        self.client_node.run(timer)
        self.client_app._execute(kv.Operation(kv.Operation.Type.GET, 'k1'),
                                 timer)
        self.assertEqual(len(self.cache_nodes[0]._inflight_messages), 3)
        self.assertEqual(len(self.cache_nodes[1]._inflight_messages), 2)
