#include "memcachekv/stats.h"

namespace memcachekv {
using namespace proto;

void
MemcacheKVStats::report_op(Operation_Type op_type, int latency, bool hit)
{
    report_latency(latency);
    this->received_replies[op_type] += 1;

    if (op_type == Operation_Type_GET) {
        if (hit) {
            this->cache_hits++;
        } else {
            this->cache_misses++;
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
    printf("GET Percentage: %.2f\n", (float)this->received_replies[Operation_Type_GET] / this->total_ops);
    printf("PUT Percentage: %.2f\n", (float)this->received_replies[Operation_Type_PUT] / this->total_ops);
    printf("DEL Percentage: %.2f\n", (float)this->received_replies[Operation_Type_DEL] / this->total_ops);
}

} // namespace memcachekv
