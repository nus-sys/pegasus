"""
message.py: Contains classes and functions for managing messages in the simulator.
"""

class Message(object):
    """
    Abstract class, representing a generic network message.
    """
    def __init__(self, length):
        self._length = length

    def length(self):
        return self._length
