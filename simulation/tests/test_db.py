"""
test_db.py: Unit tests for kv.DB
"""

import unittest
import pegasus.node
import pegasus.applications.kv as kv
import pegasus.param as param

class TestClientApp(pegasus.application.Application):
    def __init__(self):
        super().__init__()
        self.received_replies = []

    def execute(self, end_time):
        pass

    def process_message(self, message, time):
        if isinstance(message, kv.DBReply):
            self.received_replies.append(message)
        else:
            raise ValueError("Invalid message type")

class BasicTest(unittest.TestCase):
    def setUp(self):
        rack = pegasus.node.Rack()
        self.client = pegasus.node.Node(rack, 0)
        self.server = pegasus.node.Node(rack, 1)
        self.client_app = TestClientApp()
        self.server_app = kv.DB()
        self.client.register_app(self.client_app)
        self.server.register_app(self.server_app)

    def test_basic(self):
        timer = 0
        op = kv.Operation(kv.Operation.Type.PUT, "k1", "v1")
        request = kv.DBRequest(self.client, timer, op)
        self.client.send_message(request, self.server, timer)
        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.server.run(timer)
        self.client.run(timer)
        self.assertEqual(self.server_app._store['k1'], 'v1')
        self.assertEqual(len(self.client_app.received_replies), 0)

        timer += param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC
        self.server.run(timer)
        self.client.run(timer)
        self.assertEqual(len(self.client_app.received_replies), 1)
        reply = self.client_app.received_replies[0]
        self.assertEqual(reply.operation, op)
        self.assertEqual(reply.result, kv.Result.OK)

        op = kv.Operation(kv.Operation.Type.GET, "k1")
        request = kv.DBRequest(self.client, timer, op)
        self.client.send_message(request, self.server, timer)
        timer += 2 * (param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC)
        self.server.run(timer)
        self.client.run(timer)
        self.assertEqual(len(self.client_app.received_replies), 2)
        reply = self.client_app.received_replies[1]
        self.assertEqual(reply.operation, op)
        self.assertEqual(reply.result, kv.Result.OK)
        self.assertEqual(reply.value, "v1")

        op = kv.Operation(kv.Operation.Type.DEL, "k1")
        request = kv.DBRequest(self.client, timer, op)
        self.client.send_message(request, self.server, timer)
        timer += 2 * (param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC)
        self.server.run(timer)
        self.client.run(timer)
        self.assertEqual(len(self.client_app.received_replies), 3)
        reply = self.client_app.received_replies[2]
        self.assertEqual(reply.operation, op)
        self.assertEqual(reply.result, kv.Result.OK)

        op = kv.Operation(kv.Operation.Type.GET, "k1")
        request = kv.DBRequest(self.client, timer, op)
        self.client.send_message(request, self.server, timer)
        timer += 2 * (param.MAX_PROPG_DELAY + param.MAX_PKT_PROC_LTC)
        self.server.run(timer)
        self.client.run(timer)
        self.assertEqual(len(self.client_app.received_replies), 4)
        reply = self.client_app.received_replies[3]
        self.assertEqual(reply.operation, op)
        self.assertEqual(reply.result, kv.Result.NOT_FOUND)
