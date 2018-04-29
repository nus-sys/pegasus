#ifndef __MEMCACHEKV_CONFIG_H__
#define __MEMCACHEKV_CONFIG_H__

#include <vector>
#include "configuration.h"

namespace memcachekv {

uint64_t compute_keyhash(const std::string &key);

class MemcacheKVConfig : public Configuration {
public:
    enum NodeConfigMode {
        STATIC = 1,
        ROUTER = 2
    };

    MemcacheKVConfig(const std::vector<NodeAddress> &addresses,
                     const NodeAddress &router_address,
                     NodeConfigMode mode);
    MemcacheKVConfig(const char *file_path,
                     NodeConfigMode mode);
    ~MemcacheKVConfig() {};

    const NodeAddress& key_to_address(const std::string &key) override;

private:
    NodeConfigMode mode;
};

class RouterConfig : public Configuration {
public:
    enum RouterConfigMode {
        STATIC = 1
    };

    RouterConfig(const std::vector<NodeAddress> &addresses,
                 const NodeAddress &router_address,
                 RouterConfigMode mode);
    RouterConfig(const char *file_path,
                 RouterConfigMode mode);
    ~RouterConfig() {};

    const NodeAddress& key_to_address(const std::string &key) override;

private:
    RouterConfigMode mode;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CONFIG_H__ */
