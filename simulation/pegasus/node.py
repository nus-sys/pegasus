"""
node.py: Contains classes and functions for managing nodes in the simulator.
"""

from pegasus.param import *
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
    def __init__(self, id=0):
        self._nodes = set()
        self._id = id

    def id(self):
        return self._id

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
        return self.time < other.time


class Node(object):
    """
    Object representing a single node in the simulator. Each node
    belongs to a single rack.
    """
    def __init__(self, parent, id=0):
        self._parent = parent
        self._message_queue = SortedList()
        self._time = 0
        self._message_proc_remain_time = -1
        self._app = None
        self._id = id

    def _add_to_message_queue(self, message, time):
        self._message_queue.add(QueuedMessage(message, time))

    def id(self):
        return '['+str(self._parent.id())+' '+str(self._id)+']'

    def register_app(self, app):
        self._app = app
        self._app.register_node(self)

    def distance(self, node):
        return self._parent.distance(node._parent)

    def send_message(self, message, node, send_time):
        message.send_time = send_time
        arrival_time = send_time + size_distance_to_time(message.length,
                                                         self.distance(node))
        node._add_to_message_queue(message, arrival_time)

    def process_messages(self, end_time):
        """
        Process all queued messages up to ``end_time``
        """
        timer = self._time
        # Process the last message from last batch
        if self._message_proc_remain_time >= 0:
            timer += self._message_proc_remain_time
            self._message_proc_remain_time = -1
            self._app.process_message(self._message_queue[0].message, timer)
            del self._message_queue[0]

        # Now process other messages
        while len(self._message_queue) > 0:
            message = self._message_queue[0]
            if message.time > end_time:
                break
            if message.time > timer:
                timer = message.time
            timer += PKT_PROC_LTC
            if timer > end_time:
                self._message_proc_remain_time = timer - end_time
                break

            self._app.process_message(message.message, timer)
            del self._message_queue[0]

    def execute_app(self, end_time):
        """
        Execute registered application up to ``end_time``
        """
        self._app.execute(end_time)

    def run(self, end_time):
        """
        Run this node up to ``end_time``
        """
        self.process_messages(end_time)
        self.execute_app(end_time)
        self._time = end_time
