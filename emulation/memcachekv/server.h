#ifndef __MEMCACHEKV_SERVER_H__
#define __MEMCACHEKV_SERVER_H__

#include <string>
#include "application.h"

namespace memcachekv {

class Server : public Application {
public:
    Server(Transport *transport, Configuration *config);
    ~Server() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run() override;

private:
    Transport *transport;
    Configuration *config;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_SERVER_H__ */
