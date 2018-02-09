"""
test_nullrpc.py: Unit tests for the Null RPC application.
"""

import unittest
import pegasus.node
import pegasus.applications.nullrpc as nullrpc
import pegasus.param as param

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
            app.register_config(nullrpc.NullRPCConfiguration(self.nodes))
            self.apps.append(app)
            node.register_app(app)

    def test_basic(self):
        n_rounds = 4
        timer = 0
        msg_sent_sum = 0
        msg_received_sum = 0
        for i in range(n_rounds):
            timer += param.MIN_PROPG_DELAY
            for node in self.nodes:
                node.run(timer)

        # Process remaining messages
        timer += param.MAX_PROPG_DELAY + (N_NODES * param.MAX_PKT_PROC_LTC)
        for node in self.nodes:
            node.process_messages(timer)

        for app in self.apps:
            msg_sent_sum += app._sent_messages
            msg_received_sum += app._received_messages

        total_time = n_rounds * param.MIN_PROPG_DELAY
        expected_msgs = (total_time // nullrpc.MESSAGE_INTERVAL) * N_NODES

        self.assertEqual(msg_sent_sum, expected_msgs)
        self.assertEqual(msg_sent_sum, msg_received_sum)
