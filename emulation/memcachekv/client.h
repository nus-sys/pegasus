#ifndef __MEMCACHEKV_CLIENT_H__
#define __MEMCACHEKV_CLIENT_H__

#include <string>
#include "application.h"

namespace memcachekv {

class Client : public Application {
public:
    Client(Transport *transport, Configuration *config);
    ~Client() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run() override;

private:
    Transport *transport;
    Configuration *config;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CLIENT_H__ */
