#ifndef __MEMCACHEKV_STATS_H__
#define __MEMCACHEKV_STATS_H__

#include <unordered_map>
#include "appstats.h"
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

class MemcacheKVStats : public Stats {
public:
    MemcacheKVStats()
        : cache_hits(0), cache_misses(0) {};
    ~MemcacheKVStats() {};

    void report_op(proto::Operation_Type op_type, int latency, bool hit);
    void _dump() override;

private:
    uint64_t cache_hits;
    uint64_t cache_misses;
    std::unordered_map<int, uint64_t> received_replies;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_STATS_H__ */
