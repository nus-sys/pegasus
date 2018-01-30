"""
node.py: Contains classes and functions for managing nodes in the simulator.
"""

from config import *
from sortedcontainers import SortedList

def size_distance_to_time(size, distance):
    """
    Calculate message latency based on message length and
    distance between sender and receiver. Currently assume
    propagation delay is 10 * distance, and bandwidth is
    uniformly 10Gbps.
    """
    propg_delay = distance * MIN_PROPG_DELAY
    trans_delay = (size * 8) // (10*10**3)
    return propg_delay + trans_delay


class Rack(object):
    """
    Object representing a single rack in the simulator. Each rack
    contains a set of nodes.
    """
    def __init__(self):
        self._nodes = set()

    def distance(self, rack):
        """
        Distance within the same rack is 1;
        Distance across racks is 2.
        """
        if self is rack:
            return 1
        else:
            return 2

    def add_node(self, node):
        self._nodes.add(node)


class QueuedMessage(object):
    """
    Object representing a queued message in a node's
    message queue.
    """
    def __init__(self, message, time):
        self.message = message
        self.time = time

    def __lt__(self, other):
        return self._time < other._time

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    @property
    def time(self):
        return self._time

    @time.setter
    def time(self, time):
        self._time = time


class Node(object):
    """
    Object representing a single node in the simulator. Each node
    belongs to a single rack.
    """
    def __init__(self, parent):
        self._parent = parent
        self._msg_queue = SortedList()
        self._time = 0

    def _add_to_msg_queue(self, msg, time):
        self._msg_queue.add(QueuedMessage(msg, time))

    def distance(self, node):
        return self._parent.distance(node._parent)

    def send_message(self, msg, node):
        arrival_time = self._time + size_distance_to_time(msg.length(),
                                                          self.distance(node))
        node._add_to_msg_queue(msg, arrival_time)

    def process_messages(self, end_time):
        """
        Process all queued messages up to the end_time
        """
        while len(self._msg_queue) > 0:
            msg = self._msg_queue[0]
            if msg.time < end_time:
                # XXX Process message here
                del self._msg_queue[0]
            else:
                break

        self._time = end_time

