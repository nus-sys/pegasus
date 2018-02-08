"""

Clients used to test the simulator

"""

import unittest
import pegasus.node
import pegasus.applications.kv as kv
import pegasus.applications.kvimpl.memcachekv as memcachekv
from pegasus.config import *
import keyset
import config

class Client():
   Class Stats():
      # the stats are monitored using this class
      
      __init__(self, params):
         # data map of all the paramaters being monitored
         self.data = {}
         for param in params:
            self.data[param] = 0.0

      # Additional methods as needed
      def incr(self, param, num):
          self.data[param] = self.data[param] + num

      def decr(self, param, num):
          self.data[param] = self.data[param] - num


     __init__(self, dist):
        self.keyset = keyset.KeySet(config.NUM_KEYS, config.KEY_SIZE)
        self.keyset.generate_keys(self, config.DISTRIBUTION)   
     
     # the operation to be performed on the simulator
     client.execute_operation(self):
        return False

     # run the experiment for this client
     client.run(self):
        int num = 0
        while(True):
           ok = execute_operation(self)
           while (!ok and config.RETRIES):
              ok = execute_operation(self)
           num += 1
           if (config.OPERATION_NUM == num):
              break


