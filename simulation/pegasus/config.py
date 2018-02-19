"""
config.py: Configuration service for applications.
"""

class Configuration(object):
    """
    Root class of all Configuration classes. Subclass of
    ``Configuration`` should implement ``run``.
    """
    def __init__(self):
        pass

    def run(self, end_time):
        raise NotImplementedError
