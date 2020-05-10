#ifndef _STATS_H_
#define _STATS_H_

#include <sys/time.h>
#include <fstream>
#include <map>
#include <vector>
#include <atomic>
#include <mutex>

class Stats {
public:
    Stats(int n_threads,
          const char *stats_file,
          int interval,
          const char *interval_file = nullptr);
    virtual ~Stats();

    void report_issue(int tid);
    void report_latency(int tid, int latency);
    int get_latency(int tid, float percentile);
    void start();
    void done();
    void dump();
    virtual void _dump();

protected:
class ThreadStats {
public:
    ThreadStats();

    uint64_t issued_ops;
    uint64_t completed_ops;
    struct timeval last_interval;
    std::map<int, uint64_t> latencies;
    std::vector<std::map<int, uint64_t>> interval_latencies;
};

    std::vector<ThreadStats*> thread_stats;
    std::ofstream file_stream;

private:
    int interval;
    std::string interval_file;
    struct timeval start_time;
    struct timeval end_time;
};

#endif /* _STATS_H_ */
