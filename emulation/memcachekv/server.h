#ifndef __MEMCACHEKV_SERVER_H__
#define __MEMCACHEKV_SERVER_H__

#include <string>
#include <unordered_map>
#include <set>
#include <mutex>
#include <vector>
#include <tbb/concurrent_unordered_map.h>
#include "application.h"
#include "memcachekv/message.h"

namespace memcachekv {

class Server : public Application {
public:
    Server(Configuration *config, MessageCodec *codec,
           ControllerCodec *ctrl_codec, int proc_latency,
           std::string default_value, bool report_load);
    ~Server() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    void process_kv_message(const MemcacheKVMessage &msg,
                            const sockaddr &addr);
    void process_ctrl_message(const ControllerMessage &msg,
                              const sockaddr &addr);
    void process_kv_request(const MemcacheKVRequest &msg,
                            const sockaddr &addr);
    void process_op(const Operation &op,
                    MemcacheKVReply &reply);
    void migrate_kv(const Operation &op,
                    const std::string &value);
    void migrate_kv_to(const Operation &op,
                       const std::string &value,
                       int dst);
    void process_migration_request(const MigrationRequest &request);
    void update_rate(const Operation &op);
    load_t calculate_load();

    Configuration *config;
    MessageCodec *codec;
    ControllerCodec *ctrl_codec;

    struct Item {
        Item()
            : value(""), ver(0) {};
        Item(std::string value, ver_t ver)
            : value(value), ver(ver) {};
        std::string value;
        ver_t ver;
    };
    tbb::concurrent_unordered_map<std::string, Item> store;

    int proc_latency;
    std::string default_value;
    bool report_load;
    /* Load related */
    static const int EPOCH_DURATION = 1000; // 1ms
    struct timeval epoch_start;
    std::list<struct timeval> request_ts;

    static const int HK_EPOCH = 10000; // 10ms
    static const int MAX_HK_SIZE = 8;
    static const int KR_SAMPLE_RATE = 100;
    static const int HK_THRESHOLD = 5;
    unsigned int request_count;
    std::unordered_map<keyhash_t, unsigned int> key_count;
    std::unordered_map<keyhash_t, unsigned int> hk_report;
    std::mutex hk_mutex;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_SERVER_H__ */
