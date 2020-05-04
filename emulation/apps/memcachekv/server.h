#ifndef _MEMCACHEKV_SERVER_H_
#define _MEMCACHEKV_SERVER_H_

#include <string>
#include <unordered_map>
#include <set>
#include <mutex>
#include <vector>
#include <tbb/concurrent_hash_map.h>

#include <application.h>
#include <apps/memcachekv/message.h>

namespace memcachekv {

class Server : public Application {
public:
    Server(Configuration *config, MessageCodec *codec,
           ControllerCodec *ctrl_codec, int proc_latency,
           std::string default_value, bool report_load);
    ~Server();

    virtual void receive_message(const Message &msg,
                                 const Address &addr,
                                 int tid) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    void process_kv_message(const MemcacheKVMessage &msg,
                            const Address &addr,
                            int tid);
    void process_ctrl_message(const ControllerMessage &msg,
                              const Address &addr);
    void process_kv_request(const MemcacheKVRequest &request,
                            const Address &addr,
                            int tid);
    void process_op(const Operation &op,
                    MemcacheKVReply &reply,
                    int tid);
    void process_migration_request(const MigrationRequest &request);
    void process_ctrl_key_migration(const ControllerKeyMigration &key_mgr);
    void update_rate(const Operation &op, int tid);
    load_t calculate_load();

    Configuration *config;
    MessageCodec *codec;
    ControllerCodec *ctrl_codec;

    struct Item {
        Item()
            : value(""), ver(0) {};
        Item(const std::string &value, ver_t ver)
            : value(value), ver(ver) {};
        std::string value;
        ver_t ver;
    };
    tbb::concurrent_hash_map<std::string, Item> store;
    typedef tbb::concurrent_hash_map<std::string, Item>::const_accessor const_accessor_t;
    typedef tbb::concurrent_hash_map<std::string, Item>::accessor accessor_t;

    int proc_latency;
    std::string default_value;
    bool report_load;
    /* Load related */
    static const int EPOCH_DURATION = 1000; // 1ms
    struct timeval epoch_start;
    std::mutex load_mutex;
    std::list<struct timeval> request_ts;

    static const int HK_EPOCH = 10000; // 10ms
    static const int MAX_HK_SIZE = 8;
    static const int KR_SAMPLE_RATE = 100;
    static const int HK_THRESHOLD = 5;
    std::vector<unsigned> request_count;
    std::vector<tbb::concurrent_hash_map<keyhash_t, uint64_t>> key_count;
    typedef tbb::concurrent_hash_map<keyhash_t, uint64_t>::const_accessor const_kc_accessor_t;
    typedef tbb::concurrent_hash_map<keyhash_t, uint64_t>::accessor kc_accessor_t;
    std::vector<tbb::concurrent_hash_map<keyhash_t, uint64_t>> hk_report;
    typedef tbb::concurrent_hash_map<keyhash_t, uint64_t>::const_accessor const_hk_accessor_t;
    typedef tbb::concurrent_hash_map<keyhash_t, uint64_t>::accessor hk_accessor_t;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_SERVER_H_ */
