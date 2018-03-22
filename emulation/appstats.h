#ifndef __APP_STATS_H__
#define __APP_STATS_H__

#include <sys/time.h>
#include <fstream>
#include <map>

class Stats {
public:
    Stats();
    Stats(const char *stats_file);
    virtual ~Stats();

    void report_latency(int latency);
    void start();
    void done();
    void dump();
    virtual void _dump() {};

protected:
    uint64_t total_ops;
    bool record;
    std::ofstream file_stream;

private:
    struct timeval start_time;
    struct timeval end_time;
    std::map<int, uint64_t> latencies; /* sorted latency */
};

#endif /* __APP_STATS_H__ */
