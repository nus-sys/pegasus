"""
bench_memcachekv.py: MemcacheKV benchmark.
"""

import random
import string
import argparse
import enum
import sys
import copy

import pegasus.node
import pegasus.simulator
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv
import pegasus.applications.kvimpl.pegasuskv as pegasuskv
from pegasus.param import *

class KeyType(enum.Enum):
    UNIFORM = 1,
    ZIPF = 2

class IntervalType(enum.Enum):
    UNIFORM = 1,
    POISS = 2


class WorkloadGenerator(kv.KVWorkloadGenerator):
    def __init__(self, keys, value_len, get_ratio, put_ratio, key_type, interval_type, mean_interval, alpha, initkey):
        assert get_ratio + put_ratio <= 1
        self._keys = keys
        self._value = 'v'*value_len
        self._get_ratio = get_ratio
        self._put_ratio = put_ratio
        self._key_type = key_type
        self._interval_type = interval_type
        self._mean_interval = mean_interval
        self._timer = 0
        self._initkey = initkey
        if initkey:
            self._initialized_keys = set()
        if key_type == KeyType.ZIPF:
            # Generator zipf distribution data
            self._zipf = [0] * len(keys)
            c = 0.0
            for i in range(len(keys)):
                c = c + (1.0 / (i+1)**alpha)
            c = 1 / c
            sum = 0.0
            for i in range(len(keys)):
                sum += (c / (i+1)**alpha)
                self._zipf[i] = sum


    def zipf_key_index(self):
        """
        Return the next zipf key index
        """
        rand = 0
        while rand == 0:
            rand = random.random()

        l = 0
        r = len(self._keys)
        while l < r:
            mid = (l + r) // 2
            if rand > self._zipf[mid]:
                l = mid + 1
            elif rand < self._zipf[mid]:
                r = mid - 1
            else:
                break
        return mid

    def next_operation(self):
        if self._key_type == KeyType.UNIFORM:
            key = random.choice(self._keys)
        elif self._key_type == KeyType.ZIPF:
            key = self._keys[self.zipf_key_index()]
        else:
            raise ValueError("Invalide key distribution type")

        op_choice = random.uniform(0, 1)
        op_type = None
        if op_choice < self._get_ratio:
            if self._initkey:
                if key not in self._initialized_keys:
                    op_type = kv.Operation.Type.PUT
                else:
                    op_type = kv.Operation.Type.GET
            else:
                op_type = kv.Operation.Type.GET
        elif op_choice < self._get_ratio + self._put_ratio:
            op_type = kv.Operation.Type.PUT
        else:
            op_type = kv.Operation.Type.DEL

        op = None
        if op_type == kv.Operation.Type.PUT:
            if self._initkey:
                self._initialized_keys.add(key)
            op = kv.Operation(op_type, key, self._value)
        else:
            op = kv.Operation(op_type, key)

        if self._interval_type == IntervalType.UNIFORM:
            self._timer += self._mean_interval
        elif self._interval_type == IntervalType.POISS:
            self._timer += random.expovariate(1/self._mean_interval)
        else:
            raise ValueError("Invalid interval distribution type")
        return (op, self._timer)


def rand_string(str_len):
    return ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(str_len)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--alpha', type=float, default=0.5, help="zipf distribution parameter")
    parser.add_argument('-b', '--app', required=True, choices=['memcache', 'pegasus'],
                        help="application")
    parser.add_argument('-c', '--procs', type=int, required=True, help="number of processors per node")
    parser.add_argument('-d', '--duration', type=int, required=True, help="Duration of simulation (s)")
    parser.add_argument('-e', '--keytype', required=True, choices=['unif', 'zipf'],
                        help="key distribution type")
    parser.add_argument('-f', '--epoch', type=int, default = 0, help="Latency report epoch (ms)")
    parser.add_argument('-g', '--gets', type=float, required=True, help="GET ratio (0.0 to 1.0)")
    parser.add_argument('-i', '--interval', type=float, required=True, help="interval between operations (us)")
    parser.add_argument('-j', '--epochfile', default="", help="Epoch latencies output file name")
    parser.add_argument('-k', '--keys', type=int, required=True, help="number of keys")
    parser.add_argument('-l', '--length', type=int, required=True, help="key length")
    parser.add_argument('-m', '--initkey', action='store_true', help="load uninitialized keys on first GET requests")
    parser.add_argument('-n', '--nodes', type=int, required=True, help="number of cache nodes")
    parser.add_argument('-o', '--cdffile', default="", help="CDF output file name")
    parser.add_argument('-p', '--puts', type=float, required=True, help="PUT ratio (0.0 to 1.0)")
    parser.add_argument('-r', '--report', type=int, default=0, help="load balance report interval (ms)")
    parser.add_argument('-s', '--progress', action='store_true', help="Display progress bar")
    parser.add_argument('-t', '--intervaltype', required=True, choices=['unif', 'poiss'],
                        help="interval distribution type")
    parser.add_argument('-v', '--values', type=int, required=True, help="value length")
    parser.add_argument('--configtype', default='static',
                        choices=['static',
                                 'loadbalance',
                                 'boundedload',
                                 'vload',
                                 'avgload'],
                        help="configuration type for memcachekv")
    parser.add_argument('--loadbound', type=float, default=1.0, help="Bounded load configuration load constant")
    parser.add_argument('--writemode', default='update', choices=['anynode', 'update', 'invalidate'],
                        help="write mode for memcachekv")
    args = parser.parse_args()

    # Construct keys
    keys = []
    for _ in range(args.keys):
        keys.append(rand_string(args.length))

    # Initialize simulator
    if args.keytype == 'unif':
        key_type = KeyType.UNIFORM
    elif args.keytype == 'zipf':
        key_type = KeyType.ZIPF

    if args.intervaltype == 'unif':
        interval_type = IntervalType.UNIFORM
    elif args.intervaltype == 'poiss':
        interval_type = IntervalType.POISS

    generator = WorkloadGenerator(keys, args.values, args.gets, args.puts, key_type, interval_type, args.interval, args.alpha, args.initkey)
    stats = kv.KVStats(args.epoch * 1000)
    simulator = pegasus.simulator.Simulator(stats, args.progress)
    rack = pegasus.node.Rack()
    client_node = pegasus.node.Node(parent=rack,
                                    id=args.nodes,
                                    logical_client=True) # use a single logical client node
    cache_nodes = []
    for i in range(args.nodes):
        cache_nodes.append(pegasus.node.Node(parent=rack,
                                             id=i,
                                             nprocs=args.procs,
                                             drop_tail=True))

    # Register applications
    if args.app == 'memcache':
        if args.writemode == 'anynode':
            write_mode = memcachekv.WriteMode.ANYNODE
        elif args.writemode == 'update':
            write_mode = memcachekv.WriteMode.UPDATE
        elif args.writemode == 'invalidate':
            write_mode = memcachekv.WriteMode.INVALIDATE

        if args.configtype == 'static':
            config = memcachekv.StaticConfig(cache_nodes, None, write_mode)
        elif args.configtype == 'loadbalance':
            assert args.report > 0
            config = memcachekv.LoadBalanceConfig(cache_nodes,
                                                  None,
                                                  write_mode,
                                                  1000000 // MED_PKT_PROC_LTC,
                                                  args.report * 1000)
        elif args.configtype == 'boundedload':
            assert args.loadbound >= 1.0
            config = memcachekv.BoundedLoadConfig(cache_nodes,
                                                  None,
                                                  write_mode,
                                                  args.loadbound)
        elif args.configtype == 'vload':
            assert args.loadbound >= 1.0
            config = memcachekv.BoundedVirtualLoadConfig(cache_nodes,
                                                         None,
                                                         write_mode,
                                                         args.loadbound)
        elif args.configtype == 'avgload':
            assert args.loadbound >= 1.0
            config = memcachekv.BoundedAverageLoadConfig(cache_nodes,
                                                         None,
                                                         write_mode,
                                                         args.loadbound)
        client_app = memcachekv.MemcacheKVClient(generator, stats)
        server_app = memcachekv.MemcacheKVServer(None, stats)
    elif args.app == 'pegasus':
        config = pegasuskv.SingleDirectoryConfig(cache_nodes, None, 0) # cache node 0 as directory
        client_app = pegasuskv.PegasusKVClient(generator, stats)
        server_app = pegasuskv.PegasusKVServer(None, stats)

    client_app.register_config(config)
    client_node.register_app(client_app)
    for node in cache_nodes:
        app = copy.deepcopy(server_app)
        app.register_config(config)
        node.register_app(app)

    # Add nodes and config to simulator
    simulator.add_node(client_node)
    simulator.add_nodes(cache_nodes)
    simulator.register_config(config)

    # Run simulation
    simulator.run(args.duration*1000000)
    total_ops, latencies, all_epoch_latencies = stats.dump()

    # Dump latency CDF
    if len(args.cdffile) > 0:
        with open(args.cdffile, 'w') as f:
            count = 0
            for latency in sorted(latencies.keys()):
                count += latencies[latency]
                f.write(str(latency) + ' ' + str(count / total_ops) + '\n')
    # Dump epoch latencies
    if len(args.epochfile) > 0 and len(all_epoch_latencies) > 0:
        with open(args.epochfile, 'w') as f:
            time = args.epoch
            for total_ops, epoch_latencies in all_epoch_latencies:
                count = 0
                total_latency = 0
                med_latency = -1
                n_latency = -1
                nn_latency = -1
                for latency in sorted(epoch_latencies.keys()):
                    total_latency += (latency * epoch_latencies[latency])
                    count += epoch_latencies[latency]
                    if count >= total_ops // 2 and med_latency == -1:
                        med_latency = latency
                    if count >= total_ops * 0.9 and n_latency == -1:
                        n_latency = latency
                    if count >= total_ops * 0.99 and nn_latency == -1:
                        nn_latency = latency
                f.write(str(time) + ' ' + str(total_latency / total_ops) + ' ' + str(med_latency) + ' ' + str(n_latency) + ' ' + str(nn_latency) + '\n')
                time += args.epoch
