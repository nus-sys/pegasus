"""
test_simulator.py: Unit tests for the top level simulator.
"""

import unittest
import pegasus.node
import pegasus.simulator
import pegasus.applications.nullrpc as nullrpc
from pegasus.param import *

class BasicTest(unittest.TestCase):
    def setUp(self):
        self.simulator = pegasus.simulator.Simulator()
        nodes = []
        n_racks = 4
        n_nodes_per_rack = 4
        for i in range(n_racks):
            rack = pegasus.node.Rack(i)
            for j in range(n_nodes_per_rack):
                nodes.append(pegasus.node.Node(rack, j))

        config = nullrpc.NullRPCConfiguration(nodes)

        for node in nodes:
            node_app = nullrpc.NullRPC()
            node_app.register_config(config)
            node.register_app(node_app)

        self.simulator.add_nodes(nodes)

    def test_basic(self):
        assert (MIN_PROPG_DELAY // nullrpc.MESSAGE_INTERVAL) * PKT_PROC_LTC < MIN_PROPG_DELAY
        n_rounds = 4
        msg_sent_sum = 0
        msg_received_sum = 0
        timer = n_rounds * nullrpc.MESSAGE_INTERVAL

        self.simulator.run(timer)

        # Process remaining messages
        timer += (2 * MIN_PROPG_DELAY + len(self.simulator._nodes) * PKT_PROC_LTC)
        for node in self.simulator._nodes:
            node.process_messages(timer)

        for node in self.simulator._nodes:
            msg_sent_sum += node._app._sent_messages
            msg_received_sum += node._app._received_messages

        expected_msgs = n_rounds * len(self.simulator._nodes)
        self.assertEqual(msg_sent_sum, expected_msgs)
        self.assertEqual(msg_sent_sum, msg_received_sum)
