"""
stats.py: Classes and functions related to statistics collection and dumping.
"""

class Stats(object):
    """
    Abstract class. Collects application level statistics. Subclass
    of ``Stats`` should implement ``_dump``.
    """
    def __init__(self):
        self.latencies = {}
        self.total_ops = 0
        self.end_time = 0

    def report_end_time(self, end_time):
        self.end_time = end_time

    def report_latency(self, latency):
        latency = round(latency, 2)
        count = self.latencies.setdefault(latency, 0)
        self.latencies[latency] = count + 1
        self.total_ops += 1

    def _dump(self):
        raise NotImplementedError

    def dump(self):
        assert self.end_time > 0
        count = 0
        total_latency = 0
        med_latency = -1
        n_latency = -1
        nn_latency = -1
        for latency in sorted(self.latencies.keys()):
            total_latency += (latency * self.latencies[latency])
            count += self.latencies[latency]
            if count >= self.total_ops // 2 and med_latency == -1:
                med_latency = latency
            if count >= self.total_ops * 0.9 and n_latency == -1:
                n_latency = latency
            if count >= self.total_ops * 0.99 and nn_latency == -1:
                nn_latency = latency

        print("Throughput:", "{0:.2f}".format(self.total_ops / (self.end_time / 1000000)))
        print("Average Latency:", "{0:.2f}".format(total_latency / self.total_ops))
        print("Median Latency:", med_latency)
        print("90% Latency:", n_latency)
        print("99% Latency:", nn_latency)

        self._dump()
        return (self.total_ops, self.latencies)
