#include "memcachekv/stats.h"

namespace memcachekv {

void
MemcacheKVStats::report_op(Operation::Type op_type, int latency, bool hit)
{
    report_latency(latency);
    {
        std::lock_guard<std::mutex> lck(this->mtx);
        this->received_replies[op_type] += 1;

        if (op_type == Operation::Type::GET) {
            if (hit) {
                this->cache_hits++;
            } else {
                this->cache_misses++;
            }
        }
    }
}

void
MemcacheKVStats::_dump()
{
    if (this->cache_hits + this->cache_misses > 0) {
        printf("Cache Hit Rate: %.2f\n", (float)this->cache_hits / (this->cache_hits + this->cache_misses));
    } else {
        printf("Cache Hit Rate: 0\n");
    }
    printf("GET Percentage: %.2f\n", (float)this->received_replies[Operation::Type::GET] / this->completed_ops);
    printf("PUT Percentage: %.2f\n", (float)this->received_replies[Operation::Type::PUT] / this->completed_ops);
    printf("DEL Percentage: %.2f\n", (float)this->received_replies[Operation::Type::DEL] / this->completed_ops);
}

} // namespace memcachekv
