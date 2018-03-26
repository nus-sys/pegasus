#ifndef __MEMCACHEKV_SERVER_H__
#define __MEMCACHEKV_SERVER_H__

#include <string>
#include <unordered_map>
#include "application.h"
#include "memcachekv/message.h"

namespace memcachekv {

class Server : public Application {
public:
    Server(Transport *transport,
           Configuration *config,
           MessageCodec *codec,
           int proc_latency = 0)
        : transport(transport),
        config(config),
        codec(codec),
        proc_latency(proc_latency) {};
    ~Server() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    void process_op(const Operation &op,
                    MemcacheKVReply &reply);
    void migrate_key_to_node(const std::string &key,
                             int node_id);

    Transport *transport;
    Configuration *config;
    MessageCodec *codec;
    std::unordered_map<std::string, std::string> store;
    int proc_latency;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_SERVER_H__ */
