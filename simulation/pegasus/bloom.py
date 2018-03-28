"""
bloom.py: Bloom filter implementation
"""

class HashFn(object):
    def __init__(self, hasher, seed):
        self._hasher = hasher
        self._seed = seed

    def hash(self, key):
        return self._hasher(key, seed=self._seed)


class BloomFilter(object):
    def __init__(self, hash_fns, m):
        self._hash_fns = hash_fns
        self._m = m
        self._array = []
        for _ in range(m):
            self._array.append(0)

    def add(self, element):
        for hash_fn in self._hash_fns:
            index = hash_fn.hash(element) % self._m
            self._array[index] += 1

    def remove(self, element):
        for hash_fn in self._hash_fns:
            index = hash_fn.hash(element) % self._m
            if self._array[index] > 0:
                self._array[index] -= 1

    def contains(self, element):
        for hash_fn in self._hash_fns:
            index = hash_fn.hash(element) % self._m
            if self._array[index] == 0:
                return False
        return True
