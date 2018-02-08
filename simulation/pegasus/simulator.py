"""
simulator.py: Top level simulator.
"""

import copy

import pegasus.node
import pegasus.application
from pegasus.param import *

class Simulator(object):
    def __init__(self):
        self._nodes = []

    def add_node(self, node):
        self._nodes.append(node)

    def add_nodes(self, nodes):
        self._nodes.extend(nodes)

    def setup_auto(self, n_racks, n_nodes_per_rack, app):
        assert n_racks > 0
        assert n_nodes_per_rack > 0
        # Setup nodes
        for i in range(n_racks):
            rack = pegasus.node.Rack(i)
            for j in range(n_nodes_per_rack):
                node = pegasus.node.Node(rack, j)
                self._nodes.append(node)

        # Setup apps
        for node in self._nodes:
            node_app = copy.deepcopy(app)
            node_app.register_nodes(node, self._nodes)
            node.register_app(node_app)

    def run(self, duration):
        """
        Run the simulator for ``duration`` usecs.
        """
        timer = MIN_PROPG_DELAY
        while timer <= duration:
            for node in self._nodes:
                node.run(timer)

            timer += MIN_PROPG_DELAY
