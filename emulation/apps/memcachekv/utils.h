#ifndef _MEMCACHEKV_UTILS_H_
#define _MEMCACHEKV_UTILS_H_

#include <cstdint>
#include <string>

namespace memcachekv {

#define N_VIRTUAL_NODES 16

inline uint64_t compute_keyhash(const std::string &key)
{
    uint64_t hash = 5381;
    for (auto c : key) {
        hash = ((hash << 5) + hash) + (uint64_t)c;
    }
    return hash;
}

inline int key_to_node_id(const std::string &key, int num_nodes)
{
    return (int)((compute_keyhash(key) % (num_nodes * N_VIRTUAL_NODES)) % num_nodes);
}

} // namespace memcachekv

#endif /* _MEMCACHEKV_UTILS_H_ */
