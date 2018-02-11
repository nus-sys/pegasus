"""
test_node.py: Unit tests for the ``Node`` class.
"""

import unittest
import pegasus.node as node
import pegasus.message as message
import pegasus.application as application
import pegasus.param as param

class TestApp(application.Application):
    def __init__(self):
        super().__init__()
        self.received_msgs = 0

    def execute(self, end_time):
        pass

    def process_message(self, message, time):
        self.received_msgs += 1


class TwoNodesTest(unittest.TestCase):
    def setUp(self):
        self.rack = node.Rack()
        self.node_a = node.Node(self.rack)
        self.node_b = node.Node(self.rack)
        self.app_a = TestApp()
        self.app_b = TestApp()
        self.node_a.register_app(self.app_a)
        self.node_b.register_app(self.app_b)
        self.rack.add_node(self.node_a)
        self.rack.add_node(self.node_b)

    def test_single_message(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b, self.node_a._time)
        self.assertEqual(len(self.node_b._message_queue), 1)
        arrv_time = self.node_b._message_queue[0].time
        self.node_b.run(arrv_time)
        self.assertEqual(self.app_b.received_msgs, 0)
        self.node_b.run(arrv_time+param.MAX_PKT_PROC_LTC+1)
        self.assertEqual(len(self.node_b._message_queue), 0)
        self.assertEqual(self.app_b.received_msgs, 1)

    def test_multiple_messages(self):
        msg = message.Message(1024)
        self.node_a.send_message(msg, self.node_b, self.node_a._time)
        self.node_a.send_message(msg, self.node_b, self.node_a._time)
        self.node_b.send_message(msg, self.node_a, self.node_b._time)
        self.node_b.send_message(msg, self.node_a, self.node_b._time)
        self.assertEqual(len(self.node_a._message_queue), 2)
        self.assertEqual(len(self.node_b._message_queue), 2)
        timer = self.node_a._message_queue[0].time + param.MAX_PKT_PROC_LTC
        self.node_a.run(timer)
        self.assertEqual(self.app_a.received_msgs, 1)
        timer += param.MAX_PKT_PROC_LTC + (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY)
        self.node_a.run(timer)
        self.assertEqual(len(self.node_a._message_queue), 0)
        self.assertEqual(self.app_a.received_msgs, 2)

        timer = self.node_b._message_queue[0].time + param.MAX_PKT_PROC_LTC
        self.node_b.run(timer)
        self.assertEqual(self.app_b.received_msgs, 1)
        timer += param.MAX_PKT_PROC_LTC + (param.MAX_PROPG_DELAY - param.MIN_PROPG_DELAY)
        self.node_b.run(timer)
        self.assertEqual(len(self.node_b._message_queue), 0)
        self.assertEqual(self.app_b.received_msgs, 2)


class MultiProcsTest(unittest.TestCase):
    def setUp(self):
        rack = node.Rack()
        self.server_a = node.Node(rack, 1)
        self.server_b = node.Node(parent=rack, id=2, nprocs=4)
        self.app_a = TestApp()
        self.app_b = TestApp()
        self.server_a.register_app(self.app_a)
        self.server_b.register_app(self.app_b)

    def test_basic(self):
        msg = message.Message(1024)
        for _ in range(8):
            self.server_a._message_queue.add(node.QueuedMessage(msg, 0))
            self.server_b._message_queue.add(node.QueuedMessage(msg, 0))

        self.server_a.run(param.MIN_PKT_PROC_LTC-1)
        self.server_b.run(param.MIN_PKT_PROC_LTC-1)
        self.assertEqual(self.app_a.received_msgs, 0)
        self.assertEqual(self.app_b.received_msgs, 0)

        self.server_a.run(param.MAX_PKT_PROC_LTC)
        self.server_b.run(param.MAX_PKT_PROC_LTC)
        self.assertEqual(self.app_a.received_msgs, 1)
        self.assertEqual(self.app_b.received_msgs, 4)

        self.server_a.run(2*param.MAX_PKT_PROC_LTC)
        self.server_b.run(2*param.MAX_PKT_PROC_LTC)
        self.assertEqual(self.app_a.received_msgs, 2)
        self.assertEqual(self.app_b.received_msgs, 8)

class TwoRacksTest(unittest.TestCase):
    def setUp(self):
        self.rack_a = node.Rack()
        self.rack_b = node.Rack()
        self.node_a1 = node.Node(self.rack_a)
        self.node_a2 = node.Node(self.rack_a)
        self.node_b1 = node.Node(self.rack_b)
        self.node_b2 = node.Node(self.rack_b)
        self.app = TestApp()
        self.node_a1.register_app(self.app)
        self.node_a2.register_app(self.app)
        self.node_b1.register_app(self.app)
        self.node_b2.register_app(self.app)

    def test_message_order(self):
        msg_a = message.Message(1024)
        msg_b = message.Message(1024)
        self.node_a1.send_message(msg_a, self.node_b2, self.node_a1._time)
        self.node_b1.send_message(msg_b, self.node_b2, self.node_b1._time)
        self.assertEqual(len(self.node_b2._message_queue), 2)
        self.assertTrue(self.node_b2._message_queue[0].message is msg_b)
        arrv_time = self.node_b2._message_queue[0].time
        self.node_b2.run(arrv_time+param.MAX_PKT_PROC_LTC)
        self.assertEqual(len(self.node_b2._message_queue), 1)
        self.assertTrue(self.node_b2._message_queue[0].message is msg_a)

if __name__ == '__main__':
    unittest.main()
