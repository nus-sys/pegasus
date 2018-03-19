#ifndef __MEMCACHEKV_ROUTER_H__
#define __MEMCACHEKV_ROUTER_H__

#include <unordered_map>
#include "application.h"
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

class Router : public Application {
public:
    Router(Transport *transport, Configuration *config)
        : transport(transport), config(config) {};
    ~Router() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    Transport *transport;
    Configuration *config;
    std::unordered_map<int, struct sockaddr> client_addresses; /* client_id -> client address */
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_ROUTER_H__ */
