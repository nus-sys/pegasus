"""
bench_workload.py: Workload simulator.
"""

import random
import string
import argparse
import sys

def gen_zipf_dist(n, alpha):
    """
    Generate zipfian distribution with ``n`` keys and parameter ``alpha``
    """
    zipf = [0] * n
    c = 0.0
    for i in range(n):
        c = c + (1.0 / (i+1)**alpha)
    c = 1 / c
    for i in range(n):
        zipf[i] = (c / (i+1)**alpha)
    return zipf

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--servers', type=int, required=True,
                        help="number of servers per rack")
    parser.add_argument('-r', '--racks', type=int, required=True,
                        help="number fo racks")
    parser.add_argument('-k', '--keys', type=int, required=True,
                        help="number of keys")
    parser.add_argument('-a', '--alpha', type=float, required=True,
                        help="zipf distribution parameter")
    parser.add_argument('-l', '--load', type=int, required=True,
                        help="maximum load per server")
    parser.add_argument('-u', '--utilization', type=float, default=1.0,
                        help="server utilization (as decimal)")
    parser.add_argument('-f', '--file',
                        help="output file name")
    args = parser.parse_args()

    # Generate zipfian distribution
    zipf = gen_zipf_dist(args.keys, args.alpha)

    # Randomly assign keys to servers, and calculate load for each server
    keys = list(range(args.keys))
    random.shuffle(keys)
    total_load = args.load * args.racks * args.servers * args.utilization
    racks = []
    for i in range(args.racks):
        servers = [0] * args.servers # per server load
        for j in range(args.servers):
            for _ in range(int(args.keys / (args.racks * args.servers))):
                key = keys.pop()
                servers[j] += zipf[key] * total_load
        racks.append(servers)

    # Output statistics
    if args.file:
        with open(args.file, 'w') as f:
            for i in range(args.racks):
                f.write("Rack " + str(i) + '\n')
                for j in range(args.servers):
                    f.write(str(int(racks[i][j])) + '\n')
                f.write("Aggregate:" + str(int(sum(racks[i]))) + '\n')
