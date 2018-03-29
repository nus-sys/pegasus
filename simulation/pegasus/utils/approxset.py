"""
approxset.py: Approximate set implementations, including
bloom filter, and count-min sketch.
"""
import math

class HashFn(object):
    def __init__(self, hasher, seed):
        self._hasher = hasher
        self._seed = seed

    def hash(self, key):
        return self._hasher(key, seed=self._seed)


class ApproxSet(object):
    def __init__(self):
        self._n_keys = 0

    def add(self, element):
        self._n_keys += 1
        self._add(element)

    def _add(self, element):
        raise NotImplementedError

    def remove(self, element):
        if (self._n_keys > 0):
            self._n_keys -= 1
        self._remove(element)

    def _remove(self, element):
        raise NotImplementedError

    def contains(self, element):
        raise NotImplementedError

    def expected_false_positives(self):
        raise NotImplementedError


class BloomFilter(ApproxSet):
    def __init__(self, hash_fns, m):
        super().__init__()
        self._hash_fns = hash_fns
        self._m = m
        self._array = [0 for _ in range(m)]

    def _add(self, element):
        for hash_fn in self._hash_fns:
            index = hash_fn.hash(element) % self._m
            self._array[index] += 1

    def _remove(self, element):
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

    def expected_false_positives(self):
        k = len(self._hash_fns)
        m = self._m
        n = self._n_keys
        return (1-math.exp((-k*n)/m))**k


class CMSketch(ApproxSet):
    def __init__(self, hash_fns, m):
        super().__init__()
        self._hash_fns = hash_fns
        self._m = m
        self._sketch = [[0 for _ in range(m)] for _ in range(len(hash_fns))]

    def _add(self, element):
        for i in range(len(self._hash_fns)):
            index = self._hash_fns[i].hash(element) % self._m
            self._sketch[i][index] += 1

    def _remove(self, element):
        for i in range(len(self._hash_fns)):
            index = self._hash_fns[i].hash(element) % self._m
            if self._sketch[i][index] > 0:
                self._sketch[i][index] += 1

    def contains(self, element):
        for i in range(len(self._hash_fns)):
            index = self._hash_fns[i].hash(element) % self._m
            if self._sketch[i][index] == 0:
                return False
        return True

    def expected_false_positives(self):
        return 0
