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
    Stats();
    Stats(const char *stats_file, int interval, const char *interval_file = nullptr);
    virtual ~Stats();

    void report_issue();
    void report_latency(int latency);
    int get_latency(float percentile);
    void start();
    void done();
    void dump();
    virtual void _dump();

protected:
    std::atomic_ullong issued_ops;
    uint64_t completed_ops;
    std::ofstream file_stream;

private:
    int interval;
    std::mutex mtx;
    std::string interval_file;
    struct timeval start_time;
    struct timeval end_time;
    struct timeval last_interval;
    std::map<int, uint64_t> latencies; /* sorted latency */
    std::vector<std::map<int, uint64_t> > interval_latencies;
};

#endif /* _STATS_H_ */
