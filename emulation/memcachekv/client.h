#ifndef __MEMCACHEKV_CLIENT_H__
#define __MEMCACHEKV_CLIENT_H__

#include <string>
#include <vector>
#include <sys/time.h>
#include <unordered_map>
#include <random>
#include <mutex>
#include "application.h"
#include "configuration.h"
#include "memcachekv/stats.h"
#include "memcachekv/message.h"

namespace memcachekv {

struct NextOperation {
    inline NextOperation(int time, const Operation &op)
        : time(time), op(op) {};
    int time;
    Operation op;
};

enum KeyType {
    UNIFORM,
    ZIPF
};

class KVWorkloadGenerator {
public:
    KVWorkloadGenerator(const std::vector<std::string> *keys,
                        int value_len,
                        float get_ratio,
                        float put_ratio,
                        int mean_interval,
                        float alpha,
                        KeyType key_type);
    ~KVWorkloadGenerator() {};

    NextOperation next_operation();
private:
    int next_zipf_key_index();
    Operation::Type next_op_type();

    const std::vector<std::string> *keys;
    float get_ratio;
    float put_ratio;
    KeyType key_type;
    std::string value;
    std::vector<float> zipfs;
    std::default_random_engine generator;
    std::uniform_real_distribution<float> unif_real_dist;
    std::uniform_int_distribution<int> unif_int_dist;
    std::poisson_distribution<int> poisson_dist;
};

struct PendingRequest {
    Operation::Type op_type;
    struct timeval start_time;
    int received_acks;
    int expected_acks;

    inline PendingRequest()
        : op_type(Operation::Type::GET),
        received_acks(0),
        expected_acks(0) {};
};

class Client : public Application {
public:
    Client(Transport *transport,
           Configuration *config,
           MemcacheKVStats *stats,
           KVWorkloadGenerator *gen,
           MessageCodec *codec,
           int client_id);
    ~Client() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    enum Phase {
        WARMUP,
        RECORD,
        COOLDOWN
    };
    void execute_op(const Operation &op);
    void complete_op(uint32_t req_id, const PendingRequest &request, Result result);
    void insert_pending_request(uint32_t req_id, const PendingRequest &request);
    PendingRequest& get_pending_request(uint32_t req_id);
    void delete_pending_request(uint32_t req_id);

    Transport *transport;
    Configuration *config;
    MemcacheKVStats *stats;
    KVWorkloadGenerator *gen;
    MessageCodec *codec;

    int client_id;
    uint32_t req_id;
    Phase phase;
    std::unordered_map<uint32_t, PendingRequest> pending_requests;
    std::mutex pending_requests_mutex;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CLIENT_H__ */
