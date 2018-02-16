"""
node.py: Contains classes and functions for managing nodes in the simulator.
"""

import pegasus.param as param
from sortedcontainers import SortedList

def size_distance_to_time(size, distance):
    """
    Calculate message latency based on message length and
    distance between sender and receiver. Currently assume
    propagation delay is propg_delay * distance, and bandwidth
    is uniformly 10Gbps.
    """
    propg_delay = distance * param.propg_delay()
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
    belongs to a single rack. If ``logical_client`` is true, we
    simulate a logical client with infinite packet processing rate
    (zero PKT_PROC_LTC).
    """
    class UnfinishedMessage(object):
        def __init__(self, message, finish_time):
            self.message = message
            self.finish_time = finish_time

    def __init__(self, parent, id=0, nprocs=1, logical_client=False, drop_tail=False):
        self._parent = parent
        self._inflight_messages = SortedList()
        self._message_queue = []
        self._unfinished_messages = []
        self._time = 0
        self._app = None
        self._id = id
        self._nprocs = nprocs
        self._logical_client = logical_client
        self._drop_tail = drop_tail

    def _add_to_inflight_messages(self, message, time):
        self._inflight_messages.add(QueuedMessage(message, time))

    def id(self):
        return self._id

    def register_app(self, app):
        self._app = app
        self._app.register_node(self)

    def distance(self, node):
        return self._parent.distance(node._parent)

    def send_message(self, message, node, send_time):
        message.send_time = send_time
        arrival_time = send_time + size_distance_to_time(message.length,
                                                         self.distance(node))
        node._add_to_inflight_messages(message, arrival_time)

    def process_messages(self, end_time):
        """
        Process all queued messages up to ``end_time``
        """
        # Add inflight messages within end_time to the message queue
        while len(self._inflight_messages) > 0:
            message = self._inflight_messages[0]
            if message.time > end_time:
                break
            del self._inflight_messages[0]
            if self._drop_tail and not self._logical_client:
                if len(self._message_queue) < param.NODE_MSG_QUEUE_LENGTH + (end_time - self._time) // param.MIN_PKT_PROC_LTC:
                    self._message_queue.append(message)
            else:
                self._message_queue.append(message)

        proc_times = SortedList()
        # Process unfinished messages from last epoch
        if len(self._unfinished_messages) > 0:
            unfinished_messages = []
            for msg in self._unfinished_messages:
                if msg.finish_time > end_time:
                    # Still can not finish in this epoch
                    unfinished_messages.append(msg)
                else :
                    self._app.process_message(msg.message, msg.finish_time)
                    proc_times.add(msg.finish_time)
            self._unfinished_messages = unfinished_messages

        # Initialize processors' time
        while len(proc_times) + len(self._unfinished_messages) < self._nprocs:
            proc_times.add(self._time)

        # Now process other messages
        while len(self._message_queue) > 0 and len(proc_times) > 0:
            message = self._message_queue[0]
            proc_time = proc_times.pop(0)
            if message.time > proc_time:
                proc_time = message.time
            if not self._logical_client:
                proc_time += (param.pkt_proc_ltc() + self._app.message_proc_ltc(message.message))

            if proc_time > end_time:
                # Couldn't finish message in this epoch
                self._unfinished_messages.append(self.UnfinishedMessage(message.message, proc_time))
                del self._message_queue[0]
                continue

            self._app.process_message(message.message, proc_time)
            del self._message_queue[0]
            proc_times.add(proc_time)

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
