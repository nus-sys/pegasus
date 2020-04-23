#include "memcachekv/stats.h"

namespace memcachekv {

MemcacheKVStats::MemcacheKVStats(int n_threads,
                                 const char* stats_file,
                                 int interval,
                                 const char *interval_file)
    : Stats(n_threads, stats_file, interval, interval_file),
    cache_hits(n_threads, 0), cache_misses(n_threads, 0),
    replies(n_threads)
{
}

void
MemcacheKVStats::report_op(int tid, OpType op_type, int latency, bool hit)
{
    report_latency(tid, latency);
    this->replies[tid][op_type] += 1;

    if (op_type == OpType::GET) {
        if (hit) {
            this->cache_hits[tid]++;
        } else {
            this->cache_misses[tid]++;
        }
    }
}

void
MemcacheKVStats::_dump()
{
    // Combine stats from all threads
    uint64_t combined_cache_hits = 0, combined_cache_misses = 0, combined_completed_ops = 0;
    std::map<OpType, uint64_t> combined_replies;
    for (const auto &hits : this->cache_hits) {
        combined_cache_hits += hits;
    }
    for (const auto &misses : this->cache_misses) {
        combined_cache_misses += misses;
    }
    for (const auto &thread_replies : this->replies) {
        for (const auto &reply : thread_replies) {
            combined_replies[reply.first] += reply.second;
        }
    }
    for (const auto &ops : this->completed_ops) {
        combined_completed_ops += ops;
    }
    // Dump combined stats
    if (combined_cache_hits + combined_cache_misses > 0) {
        printf("Cache Hit Rate: %.2f\n", (float)combined_cache_hits / (combined_cache_hits + combined_cache_misses));
    } else {
        printf("Cache Hit Rate: 0\n");
    }
    printf("GET Percentage: %.2f\n", (float)combined_replies[OpType::GET] / combined_completed_ops);
    printf("PUT Percentage: %.2f\n", (float)combined_replies[OpType::PUT] / combined_completed_ops);
    printf("DEL Percentage: %.2f\n", (float)combined_replies[OpType::DEL] / combined_completed_ops);
}

} // namespace memcachekv
