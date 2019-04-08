#include "logger.h"
#include "memcachekv/config.h"

namespace memcachekv{

uint64_t compute_keyhash(const std::string &key)
{
    uint64_t hash = 5381;
    for (auto c : key) {
        hash = ((hash << 5) + hash) + (uint64_t)c;
    }
    return hash;
}

MemcacheKVConfig::MemcacheKVConfig(const char *file_path,
                                   NodeConfigMode mode)
    : Configuration(file_path)
{
    this->mode = mode;
}

int
MemcacheKVConfig::key_to_node_id(const std::string &key)
{
    switch (this->mode) {
    case STATIC:
    case ROUTER:
    case NETCACHE: {
        uint64_t hash = compute_keyhash(key);
        return (int)(hash % this->num_nodes);
    }
    default:
        panic("Unknown MemcacheKVConfig mode");
    }
}

RouterConfig::RouterConfig(const char *file_path,
                           RouterConfigMode mode)
    : Configuration(file_path)
{
    this->mode = mode;
}

int
RouterConfig::key_to_node_id(const std::string &key)
{
    switch (this->mode) {
    case STATIC: {
        uint64_t hash = compute_keyhash(key);
        return (int)(hash % this->num_nodes);
    }
    default:
        panic("Unknown RouterConfig mode");
    }
}

} // namespace memcachekv
