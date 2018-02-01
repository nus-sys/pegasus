"""
application.py: Contains classes and functions for managing applications in
the simulator.
"""

class Application(object):
    """
    Abstract class, representing a generic application running on
    a node. Subclass of Application should implement ``_execute``
    and ``_process_message``.
    """
    def __init__(self, node):
        self._node = node

    def execute(self, end_time):
        """
        Execute application logic up to end_time
        """
        self._execute(end_time)

    def _execute(self, end_time):
        raise NotImplementedError

    def process_message(self, message):
        """
        Process a single message
        """
        self._process_message(message)

    def _process_message(self, message):
        raise NotImplementedError
