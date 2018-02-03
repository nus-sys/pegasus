"""
message.py: Contains classes and functions for managing messages in the simulator.
"""

class Message(object):
    """
    Abstract class, representing a generic network message.
    """
    def __init__(self, length):
        self.length = length
        self.send_time = 0

    @property
    def length(self):
        return self._length

    @length.setter
    def length(self, length):
        self._length = length

    @property
    def send_time(self):
        return self._send_time

    @send_time.setter
    def send_time(self, send_time):
        self._send_time = send_time
