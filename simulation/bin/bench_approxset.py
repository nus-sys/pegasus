"""
bench_approxset.py: Approximate set benchmark
"""

import random
import pyhash
import argparse
import math
import pegasus.utils.approxset as approxset

class Simulator(object):
    def __init__(self, approxset, keys, check_keys):
        self._approxset = approxset
        self._keys = keys
        self._check_keys = check_keys

    def run(self):
        # Add all keys
        for key in self._keys:
            self._approxset.add(key)

        # Start trails. First check keys not in the set, and
        # then keys in the set.
        n_false_pos = 0
        n_false_neg = 0

        for key in self._check_keys:
            if self._approxset.contains(key):
                n_false_pos += 1

        for key in self._keys:
            if not self._approxset.contains(key):
                n_false_neg += 1

        return (n_false_pos / len(self._check_keys),
                n_false_neg / len(self._keys))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--keysfile', required=True, help="path to keys file")
    parser.add_argument('--nkeys', type=int, required=True, help="number of keys to add")
    parser.add_argument('--ntrials', type=int, required=True, help="number of trials for false positives")
    parser.add_argument('--k', type=int, required=True, help="number of hash functions")
    parser.add_argument('--m', type=int, required=True, help="approximate set size")
    parser.add_argument('--type', required=True,
                        choices=['bloom',
                                 'cmsketch'],
                        help="approximate set type")
    parser.add_argument('--outputfile', default="", help="output file")
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
        hash_fns.append(approxset.HashFn(pyhash.fnv1_32(), i))

    if args.type == 'bloom':
        aset = approxset.BloomFilter(hash_fns, args.m)
    elif args.type == 'cmsketch':
        aset = approxset.CMSketch(hash_fns, args.m // args.k)

    simulator = Simulator(aset, keys, check_keys)
    (false_pos, false_neg) = simulator.run()

    print("Expected false positive rate:", "{0:.6f}".format(aset.expected_false_positives()))
    print("False positive rate:", "{0:.6f}".format(false_pos))
    print("False negative rate:", "{0:.6f}".format(false_neg))

    if len(args.outputfile) > 0:
        with open(args.outputfile, 'a') as f:
            f.write(str(args.m) + '\t' + str(args.k) + '\t' + str(args.nkeys) + '\t' + str(false_pos) + '\t' + str(false_neg) + '\t' + str(aset.expected_false_positives()) + '\n')
