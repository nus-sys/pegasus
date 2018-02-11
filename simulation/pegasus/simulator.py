"""
simulator.py: Top level simulator.
"""

import copy
import progressbar

import pegasus.node
import pegasus.application
import pegasus.param as param

class Simulator(object):
    def __init__(self, stats, progress=False):
        self._nodes = []
        self._stats = stats
        self._progress = progress

    def add_node(self, node):
        self._nodes.append(node)

    def add_nodes(self, nodes):
        self._nodes.extend(nodes)

    def run(self, duration):
        """
        Run the simulator for ``duration`` usecs.
        """
        if self._progress:
            progress = progressbar.ProgressBar(max_value=duration).start()

        timer = param.MIN_PROPG_DELAY
        while timer <= duration:
            if self._progress:
                progress.update(timer)
            for node in self._nodes:
                node.run(timer)
            timer += param.MIN_PROPG_DELAY

        self._stats.report_end_time(timer)
        if self._progress:
            progress.finish()
