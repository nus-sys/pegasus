#ifndef _MEMCACHEKV_STATS_H_
#define _MEMCACHEKV_STATS_H_

#include <map>
#include <stats.h>
#include <memcachekv/message.h>

namespace memcachekv {

class MemcacheKVStats : public Stats {
public:
    MemcacheKVStats(int n_threads);
    MemcacheKVStats(int n_threads,
                    const char* stats_file,
                    int interval,
                    const char *interval_file = nullptr);
    ~MemcacheKVStats() {};

    void report_op(int tid, OpType op_type, int latency, bool hit);
    virtual void _dump() override;

private:
    std::vector<uint64_t> cache_hits;
    std::vector<uint64_t> cache_misses;
    std::vector<std::map<OpType, uint64_t>> replies;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_STATS_H_ */
