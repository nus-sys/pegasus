#ifndef __MEMCACHEKV_STATS_H__
#define __MEMCACHEKV_STATS_H__

#include <map>
#include "appstats.h"
#include "memcachekv/message.h"

namespace memcachekv {

class MemcacheKVStats : public Stats {
public:
    MemcacheKVStats()
        : Stats(), cache_hits(0), cache_misses(0) {};
    MemcacheKVStats(const char* stats_file)
        : Stats(stats_file), cache_hits(0), cache_misses(0) {};
    ~MemcacheKVStats() {};

    void report_op(Operation::Type op_type, int latency, bool hit);
    void _dump() override;

private:
    uint64_t cache_hits;
    uint64_t cache_misses;
    std::map<Operation::Type, uint64_t> received_replies;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_STATS_H__ */
