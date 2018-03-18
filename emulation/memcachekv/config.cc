#include "logger.h"
#include "memcachekv/config.h"

namespace memcachekv{

MemcacheKVConfig::MemcacheKVConfig(const std::vector<NodeAddress> &addresses,
                                   const NodeAddress &router_address,
                                   ConfigMode mode)
    : Configuration(addresses, router_address)
{
    this->mode = mode;
}

MemcacheKVConfig::MemcacheKVConfig(const char *file_path,
                                   ConfigMode mode)
    : Configuration(file_path)
{
    this->mode = mode;
}

const NodeAddress&
MemcacheKVConfig::key_to_address(const std::string &key)
{
    switch (this->mode) {
    case STATIC: {
        uint64_t hash = key_hash(key);
        return this->addresses[hash % this->num_nodes];
    }
    case ROUTER: {
        return this->router_address;
    }
    default:
        panic("Unknown MemcacheKVConfig mode");
    }
}

uint64_t
MemcacheKVConfig::key_hash(const std::string &key)
{
    uint64_t hash = 5381;
    for (auto c : key) {
        hash = ((hash << 5) + hash) + (uint64_t)c;
    }
    return hash;
}

} // namespace memcachekv
