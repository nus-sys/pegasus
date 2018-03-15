#ifndef __APP_STATS_H__
#define __APP_STATS_H__

#include <sys/time.h>
#include <map>

class Stats {
public:
    Stats()
        : total_ops(0) {};
    virtual ~Stats() {};

    void report_latency(int latency);
    void start();
    void done();
    void dump();
    virtual void _dump() {};

protected:
    uint64_t total_ops;

private:
    struct timeval start_time;
    struct timeval end_time;
    std::map<int, uint64_t> latencies; /* sorted latency */
};

#endif /* __APP_STATS_H__ */
