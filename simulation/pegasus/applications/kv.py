"""
kv.py: Key-value application.
"""

from enum import Enum
import pegasus.node
import pegasus.application
import pegasus.message

OP_TYPE_LEN = 1
class Operation(object):
    class Type(Enum):
        GET = 1
        PUT = 2
        DEL = 3

    def __init__(self, op_type, key, value=""):
        self.op_type = op_type
        self.key = key
        self.value = value

    def len(self):
        return OP_TYPE_LEN + len(self.key) + len(self.value)


RES_LEN = 1
class Result(Enum):
    OK = 1
    NOT_FOUND = 2


class KVWorkloadGenerator(object):
    """
    Abstract class. Defines a workload generator for the key-value
    application. Subclass of ``KVWorkloadGenerator`` should implement
    ``next_operation``.
    """
    def __init__(self):
        pass

    def next_operation(self):
        """
        Returns a (op, time) tuple, where ``op`` is an ``Operation``
        type.
        """
        raise NotImplementedError


class KV(pegasus.application.Application):
    """
    Abstract key-value application implementation. A ``None`` generator
    indicates the app runs on a server that does not generate requests.
    Subclass should implement ``_execute`` and ``_process_message``.
    """
    def __init__(self, generator):
        super().__init__()
        self._store = {}
        self._generator = generator
        if generator != None:
            self._next_op, self._next_op_time = generator.next_operation()

    def _execute_op(self, op):
        """
        Execute a single op. Returns a (result, value) tuple.
        """
        if op.op_type == Operation.Type.GET:
            if op.key in self._store:
                return (Result.OK, self._store[op.key])
            else:
                return (Result.NOT_FOUND, "")
        elif op.op_type == Operation.Type.PUT:
            self._store[op.key] = op.value
            return (Result.OK, "")
        elif op.op_type == Operation.Type.DEL:
            self._store.pop(op.key, None)
            return (Result.OK, "")
        else:
            raise ValueError("Invalid operation type")

    def _execute(self, op, time):
        """
        Either execute the operation locally, or send
        the operation to a remote node.
        """
        raise NotImplementedError

    def _process_message(self, message, time):
        raise NotImplementedError

    def execute(self, end_time):
        if self._generator != None:
            while self._next_op_time <= end_time:
                self._execute(self._next_op, self._next_op_time)
                self._next_op, self._next_op_time = generator.next_opeartion()

    def process_message(self, message, time):
        self._process_message(message, time)
