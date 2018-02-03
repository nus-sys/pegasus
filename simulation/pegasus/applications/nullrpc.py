"""
nullrpc.py: A simple Null RPC application.
"""

import random
import pegasus.application as application
import pegasus.message as message

MESSAGE_LENGTH = 1024
MESSAGE_INTERVAL = 5

class NullRPC(application.Application):
    def __init__(self, node, nodes):
        assert len(nodes) > 0
        super().__init__(node)
        self._nodes = nodes
        self._next_send_time = 0
        self._sent_messages = 0
        self._received_messages = 0

    def _execute(self, end_time):
        """
        Send message to randomly selected node every ``MESSAGE_INTERVAL``
        """
        while self._next_send_time < end_time:
            dest_node = self._node
            # Do not send to itself
            while dest_node is self._node:
                dest_node = random.choice(self._nodes)
            msg = message.Message(MESSAGE_LENGTH)
            self._node.send_message(msg, dest_node, self._next_send_time)
            self._next_send_time += MESSAGE_INTERVAL
            self._sent_messages += 1

    def _process_message(self, message, time):
        self._received_messages += 1
