"""
node_test.py: Unit tests for the Node class.
"""

import unittest
import node
import message

class TwoNodesTest(unittest.TestCase):
    def setUp(self):
        self.rack = node.Rack()
        self.node_a = node.Node(self.rack)
        self.node_b = node.Node(self.rack)
        self.rack.add_node(self.node_a)
        self.rack.add_node(self.node_b)

    def test_single_message(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b)
        self.assertEqual(len(self.node_b._msg_queue), 1)
        arrv_time = self.node_b._msg_queue[0].time
        self.node_b.process_messages(arrv_time+1)
        self.assertEqual(len(self.node_b._msg_queue), 0)


if __name__ == '__main__':
    unittest.main()
