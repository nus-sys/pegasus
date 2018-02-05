"""
application.py: Contains classes and functions for managing applications in
the simulator.
"""

class Application(object):
    """
    Abstract class, representing a generic application running on
    a node. Subclass of ``Application`` should implement
    ``execute`` and ``process_message``.
    """
    def __init__(self):
        self._local_node = None
        self._remote_nodes = None

    def register_nodes(self, local_node, remote_nodes):
        self._local_node = local_node
        self._remote_nodes = remote_nodes

    def execute(self, end_time):
        """
        Execute application logic up to end_time
        """
        raise NotImplementedError

    def process_message(self, message, time):
        """
        Process a single message
        """
        raise NotImplementedError
