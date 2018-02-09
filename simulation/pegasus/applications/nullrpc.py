"""
nullrpc.py: A simple Null RPC application.
"""

import random
import pegasus.application
import pegasus.message
import pegasus.config
import pegasus.param as param

MESSAGE_LENGTH = 1024
MESSAGES_PER_EPOCH = 2
MESSAGE_INTERVAL = param.MIN_PROPG_DELAY // MESSAGES_PER_EPOCH

class NullRPCConfiguration(pegasus.config.Configuration):
    def __init__(self, nodes):
        super().__init__()
        self.nodes = nodes

class NullRPC(pegasus.application.Application):
    def __init__(self):
        super().__init__()
        self._next_send_time = MESSAGE_INTERVAL
        self._sent_messages = 0
        self._received_messages = 0

    def execute(self, end_time):
        """
        Send message to randomly selected node every ``MESSAGE_INTERVAL``
        """
        while self._next_send_time <= end_time:
            dest_node = self._node
            # Do not send to itself
            while dest_node is self._node:
                dest_node = random.choice(self._config.nodes)
            msg = pegasus.message.Message(MESSAGE_LENGTH)
            self._node.send_message(msg, dest_node, self._next_send_time)
            self._next_send_time += MESSAGE_INTERVAL
            self._sent_messages += 1

    def process_message(self, message, time):
        self._received_messages += 1
