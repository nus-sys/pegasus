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
        ROUTER = 2,
        NETCACHE = 3
    };

    MemcacheKVConfig(const char *file_path,
                     NodeConfigMode mode);
    ~MemcacheKVConfig() {};

    int key_to_node_id(const std::string &key) override;

private:
    NodeConfigMode mode;
};

class RouterConfig : public Configuration {
public:
    enum RouterConfigMode {
        STATIC = 1
    };

    RouterConfig(const char *file_path,
                 RouterConfigMode mode);
    ~RouterConfig() {};

    int key_to_node_id(const std::string &key) override;

private:
    RouterConfigMode mode;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CONFIG_H__ */
