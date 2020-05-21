#ifndef _MEMCACHEKV_SERVER_H_
#define _MEMCACHEKV_SERVER_H_

#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include <pthread.h>
#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>

#include <application.h>
#include <apps/memcachekv/message.h>

typedef uint64_t count_t;

namespace memcachekv {

class Server : public Application {
public:
    Server(Configuration *config, MessageCodec *codec,
           ControllerCodec *ctrl_codec, int proc_latency,
           std::string default_value, bool report_load,
           std::deque<std::string> &keys);
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
    void process_replication_request(const ReplicationRequest &request);
    void process_ctrl_replication(const ControllerReplication &request);
    void update_rate(const Operation &op, int tid);
    load_t calculate_load();

    Configuration *config;
    MessageCodec *codec;
    ControllerCodec *ctrl_codec;

    struct Item {
        Item();
        Item(ver_t ver, const std::string &value);
        Item(const Item &item);

        ver_t ver;
        std::string value;
    };
    typedef tbb::concurrent_hash_map<std::string, Item>::const_accessor const_store_ac_t;
    typedef tbb::concurrent_hash_map<std::string, Item>::accessor store_ac_t;
    tbb::concurrent_hash_map<std::string, Item> store;

    int proc_latency;
    std::string default_value;
    bool report_load;
    /* Load related */
    static const int EPOCH_DURATION = 1000; // 1ms
    struct timeval epoch_start;
    std::mutex load_mutex;
    std::list<struct timeval> request_ts;
    /* Request stats */
    static const int STATS_HK_REPORT_SIZE = 8;
    static const int STATS_SAMPLE_RATE = 1000;
    static const int STATS_HK_THRESHOLD = 5;
    static const int STATS_EPOCH = 10000; // 10ms
    pthread_rwlock_t stats_lock;
    std::vector<count_t> request_count;
    tbb::concurrent_unordered_map<keyhash_t, count_t> access_count;
    tbb::concurrent_unordered_set<keyhash_t> hot_keys;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_SERVER_H_ */
