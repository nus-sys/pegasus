"""
bench_memcachekv.py: MemcacheKV benchmark.
"""

import random
import string
import argparse

import pegasus.node
import pegasus.simulator
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv

class StaticConfig(memcachekv.MemcacheKVConfiguration):
    def __init__(self, cache_nodes, db_node):
        super().__init__(cache_nodes, db_node)

    def key_to_node(self, key):
        return self.cache_nodes[hash(key) % len(self.cache_nodes)]

class UniformWorkloadGenerator(kv.KVWorkloadGenerator):
    def __init__(self, keys, value_len, get_ratio, put_ratio, interval):
        assert get_ratio + put_ratio <= 1
        self._keys = keys
        self._value = 'v'*value_len
        self._get_ratio = get_ratio
        self._put_ratio = put_ratio
        self._interval = interval
        self._timer = 0

    def next_operation(self):
        key = random.choice(self._keys)

        op_choice = random.uniform(0, 1)
        op_type = None
        if op_choice < self._get_ratio:
            op_type = kv.Operation.Type.GET
        elif op_choice < self._get_ratio + self._put_ratio:
            op_type = kv.Operation.Type.PUT
        else:
            op_type = kv.Operation.Type.DEL

        op = None
        if op_type == kv.Operation.Type.PUT:
            op = kv.Operation(op_type, key, self._value)
        else:
            op = kv.Operation(op_type, key)

        self._timer += self._interval
        return (op, self._timer)


def rand_string(str_len):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(str_len)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--keys', type=int, required=True, help="number of keys")
    parser.add_argument('-l', '--length', type=int, required=True, help="key length")
    parser.add_argument('-v', '--values', type=int, required=True, help="value length")
    parser.add_argument('-i', '--interval', type=int, required=True, help="interval between operations (us)")
    parser.add_argument('-n', '--nodes', type=int, required=True, help="number of cache nodes")
    parser.add_argument('-g', '--gets', type=float, required=True, help="GET ratio (0.0 to 1.0)")
    parser.add_argument('-p', '--puts', type=float, required=True, help="PUT ratio (0.0 to 1.0)")
    parser.add_argument('-d', '--duration', type=int, required=True, help="Duration of simulation (s)")
    args = parser.parse_args()

    # Construct keys
    keys = []
    for _ in range(args.keys):
        keys.append(rand_string(args.length))

    # Initialize simulator
    generator = UniformWorkloadGenerator(keys, args.values, args.gets, args.puts, args.interval)
    stats = kv.KVStats()
    simulator = pegasus.simulator.Simulator(stats)
    rack = pegasus.node.Rack()
    client_node = pegasus.node.Node(rack, 0, True) # use a single logical client node
    cache_nodes = []
    for i in range(args.nodes):
        cache_nodes.append(pegasus.node.Node(rack, i+1))
    config = StaticConfig(cache_nodes, None) # no DB node

    # Register applications
    client_app = memcachekv.MemcacheKV(generator, stats)
    client_app.register_config(config)
    client_node.register_app(client_app)
    for node in cache_nodes:
        app = memcachekv.MemcacheKV(None, stats)
        app.register_config(config)
        node.register_app(app)

    # Add nodes to simulator
    simulator.add_node(client_node)
    simulator.add_nodes(cache_nodes)

    # Run simulation
    simulator.run(args.duration*1000000)
    stats.dump()
