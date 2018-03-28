"""
bench_bloom.py: Bloom filter benchmark
"""

import random
import pyhash
import argparse
import math
import pegasus.bloom as bloom

class Simulator(object):
    def __init__(self, bloom_filter, keys, check_keys):
        self._bloom_filter = bloom_filter
        self._keys = keys
        self._check_keys = check_keys

    def run(self):
        # Add all keys
        for key in self._keys:
            self._bloom_filter.add(key)

        # Start trails. First check keys not in the set, and
        # then keys in the set.
        n_false_pos = 0
        n_false_neg = 0

        for key in self._check_keys:
            if self._bloom_filter.contains(key):
                n_false_pos += 1

        for key in self._keys:
            if not self._bloom_filter.contains(key):
                n_false_neg += 1

        return (n_false_pos / len(self._check_keys),
                n_false_neg / len(self._keys))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--keysfile', required=True, help="path to keys file")
    parser.add_argument('--nkeys', type=int, required=True, help="number of keys to add")
    parser.add_argument('--ntrials', type=int, required=True, help="number of trials for false positives")
    parser.add_argument('--k', type=int, required=True, help="number of hash functions")
    parser.add_argument('--m', type=int, required=True, help="bloom filter size")
    args = parser.parse_args()

    keys = []
    check_keys = []
    with open(args.keysfile) as f:
        for _ in range(args.nkeys):
            keys.append(f.readline())

        for _ in range(args.ntrials):
            check_keys.append(f.readline())

    hash_fns = []
    for i in range(args.k):
        hash_fns.append(bloom.HashFn(pyhash.fnv1_32(), i))
    bloom_filter = bloom.BloomFilter(hash_fns, args.m)

    simulator = Simulator(bloom_filter, keys, check_keys)
    (false_pos, false_neg) = simulator.run()
    expected_false_pos = (1-math.exp((-args.k*args.nkeys)/args.m))**args.k

    print("Expected false positive rate:", "{0:.6f}".format(expected_false_pos))
    print("False positive rate:", "{0:.6f}".format(false_pos))
    print("False negative rate:", "{0:.6f}".format(false_neg))
