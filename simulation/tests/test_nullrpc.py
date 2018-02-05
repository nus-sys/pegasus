"""
test_nullrpc.py: Unit tests for the Null RPC application.
"""

import unittest
import pegasus.node
import pegasus.applications.nullrpc as nullrpc
from pegasus.config import *

N_NODES = 4

class BasicTest(unittest.TestCase):
    def setUp(self):
        self.rack = pegasus.node.Rack()
        self.nodes = []
        self.apps = []
        for i in range(N_NODES):
            self.nodes.append(pegasus.node.Node(self.rack, i))

        for node in self.nodes:
            app = nullrpc.NullRPC()
            app.register_nodes(node, self.nodes)
            self.apps.append(app)
            node.register_app(app)

    def test_basic(self):
        assert (MIN_PROPG_DELAY // nullrpc.MESSAGE_INTERVAL) * PKT_PROC_LTC < MIN_PROPG_DELAY
        n_rounds = 4
        timer = MIN_PROPG_DELAY
        msg_sent_sum = 0
        msg_received_sum = 0
        for i in range(n_rounds):
            for node in self.nodes:
                node.run(timer)
            timer += MIN_PROPG_DELAY

        # Process remaining messages
        timer += (N_NODES * PKT_PROC_LTC)
        for node in self.nodes:
            node.process_messages(timer)

        for app in self.apps:
            msg_sent_sum += app._sent_messages
            msg_received_sum += app._received_messages

        total_time = n_rounds * MIN_PROPG_DELAY
        expected_msgs = (total_time // nullrpc.MESSAGE_INTERVAL) * N_NODES

        self.assertEqual(msg_sent_sum, expected_msgs)
        self.assertEqual(msg_sent_sum, msg_received_sum)
