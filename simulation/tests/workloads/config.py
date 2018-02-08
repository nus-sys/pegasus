"""

Configuration used by all the clients in the workload

"""
import keyset

NUM_KEYS = 1000;

# key length in characters
# keep this number big so that the keys generated are distinct
KEY_SIZE = 64;

# format of a single set of operations
OPERATION_FORMAT = ['GET', 'PUT', 'GET']

# boolean to indicate wether a failed operation should retry
RETRIES = False

# Number of times a set of get/puts given by the format is to be performed 
# -1 indicates indefinitely -- techincally don't stop still termination 
OPERATION_NUM = -1

# How the operations access the keys
# 1 full set of operations per key, in an uniform manner
# meaning all keys are accessed in a scan. 
KEY_DISTRIBUTION = keyset.UNIFORM



