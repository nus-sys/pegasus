#ifndef _MEMCACHEKV_UTILS_H_
#define _MEMCACHEKV_UTILS_H_

#include <cstdint>
#include <string>

namespace memcachekv {

#define N_VIRTUAL_NODES 16
#define KEYHASH_MASK 0x7FFFFFFF
#define KEYHASH_RANGE 0x80000000

inline uint32_t compute_keyhash(const std::string &key)
{
    uint64_t hash = 5381;
    for (auto c : key) {
        hash = ((hash << 5) + hash) + (uint64_t)c;
    }
    return (uint32_t)(hash & KEYHASH_MASK);
}

inline int key_to_node_id(const std::string &key, int num_nodes)
{
    uint32_t keyhash = compute_keyhash(key);
    uint32_t interval = (uint32_t)KEYHASH_RANGE / (num_nodes * N_VIRTUAL_NODES);
    return (int)((keyhash / interval) % num_nodes);
}

} // namespace memcachekv

#endif /* _MEMCACHEKV_UTILS_H_ */
