"""
node_test.py: Unit tests for the Node class.
"""

import unittest
import pegasus.node as node
import pegasus.message as message
import pegasus.application as application
from pegasus.config import *

class TestApp(application.Application):
    def __init__(self, node):
        super().__init__(node)

    def _execute(self, end_time):
        pass

    def _process_message(self, message):
        pass


class TwoNodesTest(unittest.TestCase):
    def setUp(self):
        self.rack = node.Rack()
        self.node_a = node.Node(self.rack)
        self.node_b = node.Node(self.rack)
        self.app = TestApp(self.node_a)
        self.node_a.register_app(self.app)
        self.node_b.register_app(self.app)
        self.rack.add_node(self.node_a)
        self.rack.add_node(self.node_b)

    def test_single_message(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b)
        self.assertEqual(len(self.node_b._message_queue), 1)
        arrv_time = self.node_b._message_queue[0].time
        self.node_b.run(arrv_time)
        self.assertEqual(len(self.node_b._message_queue), 1)
        self.node_b.run(arrv_time+PKT_PROC_LTC+1)
        self.assertEqual(len(self.node_b._message_queue), 0)

    def test_multiple_messages(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b)
        self.node_a.send_message(msg, self.node_b)
        self.node_b.send_message(msg, self.node_a)
        self.node_b.send_message(msg, self.node_a)
        self.assertEqual(len(self.node_a._message_queue), 2)
        self.assertEqual(len(self.node_b._message_queue), 2)
        timer = self.node_a._message_queue[0].time
        timer += PKT_PROC_LTC
        self.node_a.run(timer)
        self.node_b.run(timer)
        self.assertEqual(len(self.node_a._message_queue), 1)
        self.assertEqual(len(self.node_b._message_queue), 1)
        timer += PKT_PROC_LTC
        self.node_a.run(timer)
        self.node_b.run(timer)
        self.assertEqual(len(self.node_a._message_queue), 0)
        self.assertEqual(len(self.node_b._message_queue), 0)


class TwoRacksTest(unittest.TestCase):
    def setUp(self):
        self.rack_a = node.Rack()
        self.rack_b = node.Rack()
        self.node_a1 = node.Node(self.rack_a)
        self.node_a2 = node.Node(self.rack_a)
        self.node_b1 = node.Node(self.rack_b)
        self.node_b2 = node.Node(self.rack_b)
        self.app = TestApp(self.node_a1)
        self.node_a1.register_app(self.app)
        self.node_a2.register_app(self.app)
        self.node_b1.register_app(self.app)
        self.node_b2.register_app(self.app)

    def test_message_order(self):
        msg_a = message.Message(1024)
        msg_b = message.Message(1024)
        self.node_a1.send_message(msg_a, self.node_b2)
        self.node_b1.send_message(msg_b, self.node_b2)
        self.assertEqual(len(self.node_b2._message_queue), 2)
        self.assertTrue(self.node_b2._message_queue[0].message is msg_b)
        arrv_time = self.node_b2._message_queue[0].time
        self.node_b2.run(arrv_time+PKT_PROC_LTC+1)
        self.assertEqual(len(self.node_b2._message_queue), 1)
        self.assertTrue(self.node_b2._message_queue[0].message is msg_a)

if __name__ == '__main__':
    unittest.main()
