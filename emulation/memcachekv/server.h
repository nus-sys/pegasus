#ifndef __MEMCACHEKV_SERVER_H__
#define __MEMCACHEKV_SERVER_H__

#include <string>
#include <unordered_map>
#include "application.h"
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

class Server : public Application {
public:
    Server(Transport *transport, Configuration *config)
        : transport(transport), config(config) {};
    ~Server() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    void process_op(const proto::Operation &op,
                    proto::MemcacheKVReply &reply);

    Transport *transport;
    Configuration *config;
    std::unordered_map<std::string, std::string> store;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_SERVER_H__ */
