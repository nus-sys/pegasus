from pegasus.config import *
from collections import deque
import string
import random

UNIFORM = 1
RANDOM = 0

class KeySet:
   def __init__(self, num_keys, key_size):
      assert num_keys > 0
      self.ksize = key_size
      self.nkeys = num_keys
      self.keys = deque

   def generate_keys(self, dist):
      self.distribution = dist
      if (dist != RANDOM or dist != UNIFORM):
         raise NotImplementedError
      keyset = set()

      for (len(keys) < self.nkeys):
         key = ""
         for (j = 0; j < self.ksize; j++):
            key += random.choice(string.letters)
         keyset.add(key)

      # Add the set of random keys to a circular queue
      for key in keyset:
         self.keys.append(key)
      return

   def next_uniform_key(self):
       nkey = self.keys.popleft()
       self.keys.append(nkey)
       return nkey

   def next_random_key(self):
       nrand = random.randint(0, len(q))
       self.keys.rotate(nrand)
       return self.next_uniform_key()

   def next(self):
      if self.distribution == RANDOM:
         return self.next_random_key()
      elif self.distribution == UNIFORM:
         return self.next_uniform_key()
      else:
         raise NotImplementedError
