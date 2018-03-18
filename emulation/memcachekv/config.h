#ifndef __MEMCACHEKV_CONFIG_H__
#define __MEMCACHEKV_CONFIG_H__

#include <vector>
#include "configuration.h"

namespace memcachekv {

enum ConfigMode {
    STATIC = 1,
    ROUTER = 2
};

class MemcacheKVConfig : public Configuration {
public:
    MemcacheKVConfig(const std::vector<NodeAddress> &addresses,
                     const NodeAddress &router_address,
                     ConfigMode mode);
    MemcacheKVConfig(const char *file_path,
                     ConfigMode mode);
    ~MemcacheKVConfig() {};

    const NodeAddress& key_to_address(const std::string &key) override;

private:
    uint64_t key_hash(const std::string &key);

    ConfigMode mode;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CONFIG_H__ */
