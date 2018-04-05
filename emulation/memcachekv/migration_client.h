#ifndef __MEMCACHEKV_MIGRATION_CLIENT_H__
#define __MEMCACHEKV_MIGRATION_CLIENT_H__

#include <string>
#include <random>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include "application.h"
#include "appstats.h"
#include "configuration.h"
#include "memcachekv/message.h"

namespace memcachekv {

std::string random_string(int len);

class MigrationClient : public Application {
public:
    MigrationClient(Transport *transport,
                    Configuration *config,
                    MessageCodec *codec,
                    Stats *stats,
                    int nkeys,
                    int key_len,
                    int value_len);
    ~MigrationClient() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    enum Phase {
        WARMUP,
        RECORD,
        COOLDOWN
    };
    Transport *transport;
    Configuration *config;
    MessageCodec *codec;
    Stats *stats;
    Phase phase;
    std::vector<std::string> keys;
    std::unordered_map<std::string, std::string> store;
    std::mutex mtx;
    std::condition_variable cv;
    bool done;

    void migrate();
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MIGRATION_CLIENT_H__ */
