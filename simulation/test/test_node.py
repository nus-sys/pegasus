"""
node_test.py: Unit tests for the Node class.
"""

import unittest
import pegasus.node as node
import pegasus.message as message

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
        self.node_b.process_messages(arrv_time-1)
        self.assertEqual(len(self.node_b._msg_queue), 1)
        self.node_b.process_messages(arrv_time+1)
        self.assertEqual(len(self.node_b._msg_queue), 0)

    def test_multiple_messages(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b)
        self.node_a.send_message(msg, self.node_b)
        self.node_b.send_message(msg, self.node_a)
        self.node_b.send_message(msg, self.node_a)
        self.assertEqual(len(self.node_a._msg_queue), 2)
        self.assertEqual(len(self.node_b._msg_queue), 2)
        arrv_time = self.node_a._msg_queue[0].time
        self.node_a.process_messages(arrv_time+1)
        self.node_b.process_messages(arrv_time+1)
        self.assertEqual(len(self.node_a._msg_queue), 0)
        self.assertEqual(len(self.node_b._msg_queue), 0)

class TwoRacksTest(unittest.TestCase):
    def setUp(self):
        self.rack_a = node.Rack()
        self.rack_b = node.Rack()
        self.node_a1 = node.Node(self.rack_a)
        self.node_a2 = node.Node(self.rack_a)
        self.node_b1 = node.Node(self.rack_b)
        self.node_b2 = node.Node(self.rack_b)

    def test_message_order(self):
        msg_a = message.Message(1024)
        msg_b = message.Message(1024)
        self.node_a1.send_message(msg_a, self.node_b2)
        self.node_b1.send_message(msg_b, self.node_b2)
        self.assertEqual(len(self.node_b2._msg_queue), 2)
        self.assertTrue(self.node_b2._msg_queue[0].message is msg_b)
        arrv_time = self.node_b2._msg_queue[0].time
        self.node_b2.process_messages(arrv_time+1)
        self.assertEqual(len(self.node_b2._msg_queue), 1)
        self.assertTrue(self.node_b2._msg_queue[0].message is msg_a)

if __name__ == '__main__':
    unittest.main()
