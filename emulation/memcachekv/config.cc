#include "logger.h"
#include "memcachekv/config.h"

namespace memcachekv{

uint64_t key_hash(const std::string &key)
{
    uint64_t hash = 5381;
    for (auto c : key) {
        hash = ((hash << 5) + hash) + (uint64_t)c;
    }
    return hash;
}

MemcacheKVConfig::MemcacheKVConfig(const std::vector<NodeAddress> &addresses,
                                   const NodeAddress &router_address,
                                   NodeConfigMode mode)
    : Configuration(addresses, router_address)
{
    this->mode = mode;
}

MemcacheKVConfig::MemcacheKVConfig(const char *file_path,
                                   NodeConfigMode mode)
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

RouterConfig::RouterConfig(const std::vector<NodeAddress> &addresses,
                           const NodeAddress &router_address,
                           RouterConfigMode mode)
    : Configuration(addresses, router_address)
{
    this->mode = mode;
}

RouterConfig::RouterConfig(const char *file_path,
                           RouterConfigMode mode)
    : Configuration(file_path)
{
    this->mode = mode;
}

const NodeAddress&
RouterConfig::key_to_address(const std::string &key)
{
    switch (this->mode) {
    case STATIC: {
        uint64_t hash = key_hash(key);
        return this->addresses[hash % this->num_nodes];
    }
    default:
        panic("Unknown RouterConfig mode");
    }
}

} // namespace memcachekv
