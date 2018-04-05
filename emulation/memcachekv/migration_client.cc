#include <random>
#include "logger.h"
#include "memcachekv/migration_client.h"
#include "utils.h"

using std::string;

namespace memcachekv {

string random_string(int len) {
    static const char alphanum [] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    static std::default_random_engine generator;
    static std::uniform_int_distribution<int> distribution(0, sizeof(alphanum)-2);

    string res;
    for (int i = 0; i < len; i++) {
        res.push_back(alphanum[distribution(generator)]);
    }
    return res;
}

MigrationClient::MigrationClient(Transport *transport,
                                 Configuration *config,
                                 MessageCodec *codec,
                                 Stats *stats,
                                 int nkeys,
                                 int key_len,
                                 int value_len)
    : transport(transport), config(config), codec(codec), stats(stats), phase(WARMUP)
{
    for (int i = 0; i < nkeys; i++) {
        string key = random_string(key_len);
        string value = random_string(value_len);
        this->keys.push_back(key);
        this->store[key] = value;
    }
}

void
MigrationClient::receive_message(const string &message, const sockaddr &src_addr)
{
    if (strcmp(message.c_str(), "DONE") == 0) {
        std::unique_lock<std::mutex> lck(this->mtx);
        this->done = true;
        this->cv.notify_all();
    } else {
        panic("Migration client received unexpected message");
    }
}

void
MigrationClient::run(int duration)
{
    struct timeval start, begin, now;
    gettimeofday(&start, nullptr);

    do {
        gettimeofday(&begin, nullptr);
        migrate();
        gettimeofday(&now, nullptr);
        this->stats->report_latency(latency(begin, now));

        switch (this->phase) {
        case WARMUP: {
            if (latency(start, now) > (duration * 200000)) {
                this->phase = RECORD;
                this->stats->start();
            }
            break;
        }
        case RECORD: {
            if (latency(start, now) > (duration * 800000)) {
                this->phase = COOLDOWN;
                this->stats->done();
            }
            break;
        }
        default:
            break;
        }
    } while (latency(start, now) < duration * 1000000);

    this->stats->dump();
}

void
MigrationClient::migrate()
{
    MemcacheKVMessage request;
    request.type = MemcacheKVMessage::MIGRATION_REQUEST;
    for (const auto &key : this->keys) {
        request.migration_request.ops.push_back(Operation());
        Operation &op = request.migration_request.ops.back();
        op.op_type = Operation::PUT;
        op.key = key;
        op.value = this->store[key];
    }
    string msg;
    this->codec->encode(msg, request);

    std::unique_lock<std::mutex> lck(this->mtx);
    this->done = false;
    this->transport->send_message_to_node(msg, 0);
    while (!done) {
        this->cv.wait(lck);
    }
}

} // namespace memcachekv
