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
        self._node = None
        self._config = None

    def register_node(self, node):
        """
        Called by node when running ``register_app``.
        """
        self._node = node

    def register_config(self, config):
        self._config = config

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
