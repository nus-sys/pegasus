#ifndef _MEMCACHEKV_CLIENT_H_
#define _MEMCACHEKV_CLIENT_H_

#include <string>
#include <vector>
#include <deque>
#include <sys/time.h>
#include <unordered_map>
#include <random>
#include <mutex>

#include <application.h>
#include <configuration.h>
#include <apps/memcachekv/stats.h>
#include <apps/memcachekv/message.h>

namespace memcachekv {

struct NextOperation {
    inline NextOperation() {};
    inline NextOperation(int time, const Operation &op)
        : time(time), op(op) {};
    int time;
    Operation op;
};

enum class KeyType {
    UNIFORM,
    ZIPF
};

enum class SendMode {
    FIXED,
    DYNAMIC
};

enum class DynamismType {
    NONE,
    HOTIN,
    RANDOM
};

class KVWorkloadGenerator {
public:
    KVWorkloadGenerator(std::deque<std::string> *keys,
                        int value_len,
                        float get_ratio,
                        float put_ratio,
                        int mean_interval,
                        int target_latency,
                        float alpha,
                        KeyType key_type,
                        SendMode send_mode,
                        DynamismType d_type,
                        int d_interval,
                        int d_nkeys,
                        int n_threads,
                        Stats *stats);
    ~KVWorkloadGenerator() {};

    const NextOperation &next_operation(int tid);

private:
    int next_zipf_key_index(int tid);
    OpType next_op_type(int tid);
    void change_keys();
    void adjust_send_rate(int tid);

    std::deque<std::string> *keys;
    float get_ratio;
    float put_ratio;
    int target_latency;
    KeyType key_type;
    SendMode send_mode;
    DynamismType d_type;
    int d_interval;
    int d_nkeys;
    Stats *stats;

    NextOperation next_op;

    std::string value;
    std::vector<float> zipfs;
    struct timeval last_interval;

    // Per thread
    std::vector<int> op_count;
    std::vector<int> mean_interval;
    std::vector<std::default_random_engine> generator;
    std::vector<std::uniform_real_distribution<float>> unif_real_dist;
    std::vector<std::uniform_int_distribution<int>> unif_int_dist;
    std::vector<std::poisson_distribution<int>> poisson_dist;
};

struct PendingRequest {
    OpType op_type;
    struct timeval start_time;
    int received_acks;
    int expected_acks;

    inline PendingRequest()
        : op_type(OpType::GET),
        received_acks(0),
        expected_acks(0) {};
};

class Client : public Application {
public:
    Client(Configuration *config,
           MemcacheKVStats *stats,
           KVWorkloadGenerator *gen,
           MessageCodec *codec);
    ~Client();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    void execute_op(const Operation &op);
    void complete_op(const MemcacheKVReply &reply);
    void insert_pending_request(uint32_t req_id, const PendingRequest &request);
    PendingRequest& get_pending_request(uint32_t req_id);
    void delete_pending_request(uint32_t req_id);

    Configuration *config;
    MemcacheKVStats *stats;
    KVWorkloadGenerator *gen;
    MessageCodec *codec;

    std::atomic_uint req_id;
    std::unordered_map<uint32_t, PendingRequest> pending_requests;
    std::mutex pending_requests_mutex;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_CLIENT_H_ */
