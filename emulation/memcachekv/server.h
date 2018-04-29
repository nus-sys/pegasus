#ifndef __MEMCACHEKV_SERVER_H__
#define __MEMCACHEKV_SERVER_H__

#include <string>
#include <map>
#include <unordered_map>
#include <set>
#include "application.h"
#include "memcachekv/message.h"

namespace memcachekv {

class Server : public Application {
public:
    Server(Transport *transport,
           Configuration *config,
           MessageCodec *codec,
           ControllerCodec *ctrl_codec,
           int node_id,
           int proc_latency = 0)
        : transport(transport),
        config(config),
        codec(codec),
        ctrl_codec(ctrl_codec),
        node_id(node_id),
        proc_latency(proc_latency) {};
    ~Server() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    void process_kv_message(const MemcacheKVMessage &msg,
                            const sockaddr &addr);
    void process_ctrl_message(const ControllerMessage &msg,
                              const sockaddr &addr);
    void process_op(const Operation &op,
                    MemcacheKVReply &reply);
    void process_ctrl_migration(const ControllerMigrationRequest &msg);
    void process_ctrl_register(const ControllerRegisterReply &msg);
    bool keyhash_in_range(keyhash_t keyhash);
    void insert_kv(const std::string &key, keyhash_t keyhash, const std::string &value);
    void remove_kv(const std::string &key, keyhash_t keyhash);

    Transport *transport;
    Configuration *config;
    MessageCodec *codec;
    ControllerCodec *ctrl_codec;
    std::set<KeyRange, KeyRangeComparator> key_ranges;
    std::map<keyhash_t, std::set<std::string> > key_hashes;
    std::unordered_map<std::string, std::string> store;
    int node_id;
    int proc_latency;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_SERVER_H__ */
