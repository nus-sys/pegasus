"""
stats.py: Classes and functions related to statistics collection and dumping.
"""

class Stats(object):
    """
    Abstract class. Collects application level statistics. Subclass
    of ``Stats`` should implement ``_dump``.
    """
    def __init__(self):
        self.latencies = []
        self.end_time = 0

    def report_end_time(self, end_time):
        self.end_time = end_time

    def report_latency(self, latency):
        self.latencies.append(latency)

    def _dump(self):
        raise NotImplementedError

    def dump(self):
        assert self.end_time > 0
        self.latencies.sort()
        print("Throughput:", "{0:.2f}".format(len(self.latencies) / (self.end_time / 1000000)))
        print("Average Latency:", "{0:.2f}".format(sum(self.latencies) / len(self.latencies)))
        print("Median Latency:", self.latencies[len(self.latencies)//2])
        print("90% Latency:", self.latencies[int(len(self.latencies) * 0.9)])
        print("99% Latency:", self.latencies[int(len(self.latencies) * 0.99)])

        self._dump()
