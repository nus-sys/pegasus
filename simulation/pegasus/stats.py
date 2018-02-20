"""
stats.py: Classes and functions related to statistics collection and dumping.
"""
import copy

class Stats(object):
    """
    Abstract class. Collects application level statistics. Subclass
    of ``Stats`` should implement ``_dump``.
    """
    def __init__(self, epoch_len=0):
        self.latencies = {}
        self.total_ops = 0
        self.end_time = 0
        self.epoch_len = epoch_len
        self.last_epoch = 0
        self.epoch_latencies = {}
        self.epoch_total_ops = 0
        self.all_epoch_latencies = []

    def report_end_time(self, end_time):
        self.end_time = end_time

    def report_latency(self, latency):
        latency = round(latency)
        count = self.latencies.setdefault(latency, 0)
        self.latencies[latency] = count + 1
        self.total_ops += 1
        # Update epoch latencies
        if self.epoch_len > 0:
            count = self.epoch_latencies.setdefault(latency, 0)
            self.epoch_latencies[latency] = count + 1
            self.epoch_total_ops += 1

    def run(self, time):
        if self.epoch_len > 0:
            if time - self.last_epoch > self.epoch_len:
                self.all_epoch_latencies.append((self.epoch_total_ops, copy.deepcopy(self.epoch_latencies)))
                self.epoch_latencies.clear()
                self.epoch_total_ops = 0
                self.last_epoch = time

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
        return (self.total_ops, self.latencies, self.all_epoch_latencies)
