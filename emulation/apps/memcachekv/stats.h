#ifndef _MEMCACHEKV_STATS_H_
#define _MEMCACHEKV_STATS_H_

#include <map>
#include <stats.h>
#include <memcachekv/message.h>

namespace memcachekv {

class MemcacheKVStats : public Stats {
public:
    MemcacheKVStats()
        : Stats(), cache_hits(0), cache_misses(0) {};
    MemcacheKVStats(const char* stats_file, int interval, const char *interval_file = nullptr)
        : Stats(stats_file, interval, interval_file), cache_hits(0), cache_misses(0) {};
    ~MemcacheKVStats() {};

    void report_op(Operation::Type op_type, int latency, bool hit);
    virtual void _dump() override;

private:
    uint64_t cache_hits;
    uint64_t cache_misses;
    std::mutex mtx;
    std::map<Operation::Type, uint64_t> received_replies;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_STATS_H_ */
