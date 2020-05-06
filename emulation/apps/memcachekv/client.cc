#include <cassert>
#include <sys/time.h>
#include <thread>

#include <utils.h>
#include <logger.h>
#include <apps/memcachekv/client.h>
#include <apps/memcachekv/utils.h>

static thread_local uint32_t req_id = 1;

#define LATENCY_CHECK_COUNT 100
#define LATENCY_CHECK_PTILE 0.99
#define MIN_INTERVAL 1000

using std::string;

namespace memcachekv {

KVWorkloadGenerator::KVWorkloadGenerator(std::deque<std::string> *keys,
                                         int value_len,
                                         float get_ratio,
                                         float put_ratio,
                                         long mean_interval,
                                         int target_latency,
                                         float alpha,
                                         KeyType key_type,
                                         SendMode send_mode,
                                         DynamismType d_type,
                                         int d_interval,
                                         int d_nkeys,
                                         int n_threads,
                                         Stats *stats)
    : keys(keys), get_ratio(get_ratio), put_ratio(put_ratio),
    target_latency(target_latency), key_type(key_type), send_mode(send_mode),
    d_type(d_type), d_interval(d_interval), d_nkeys(d_nkeys), stats(stats)
{
    this->value = string(value_len, 'v');
    if (key_type == KeyType::ZIPF) {
        // Generate zipf distribution data
        float c = 0;
        for (unsigned int i = 0; i < keys->size(); i++) {
            c = c + (1.0 / pow((float)(i+1), alpha));
        }
        c = 1 / c;
        float sum = 0;
        for (unsigned int i = 0; i< keys->size(); i++) {
            sum += (c / pow((float)(i+1), alpha));
            this->zipfs.push_back(sum);
        }
    }
    struct timeval time;
    gettimeofday(&time, nullptr);
    this->last_interval = time;

    // Per thread initialization
    for (int i = 0; i < n_threads; i++) {
        this->mean_interval.push_back(mean_interval);
        this->op_count.push_back(0);
        this->generator.push_back(std::default_random_engine(time.tv_sec * 1000000 + time.tv_usec + i));
        this->unif_real_dist.push_back(std::uniform_real_distribution<float>(0.0, 1.0));
        this->unif_int_dist.push_back(std::uniform_int_distribution<int>(0, keys->size()-1));
        this->poisson_dist.push_back(std::poisson_distribution<long>(mean_interval));
    }
}

int KVWorkloadGenerator::next_zipf_key_index(int tid)
{
    float random = 0.0;
    while (random == 0.0) {
        random = this->unif_real_dist.at(tid)(this->generator.at(tid));
    }

    int l = 0, r = this->keys->size(), mid = 0;
    while (l < r) {
        mid = (l + r) / 2;
        if (random > this->zipfs.at(mid)) {
            l = mid + 1;
        } else if (random < this->zipfs.at(mid)) {
            r = mid - 1;
        } else {
            break;
        }
    }
    return mid;
}

OpType KVWorkloadGenerator::next_op_type(int tid)
{
    float op_choice = this->unif_real_dist.at(tid)(this->generator.at(tid));
    OpType op_type;
    if (op_choice < this->get_ratio) {
        op_type = OpType::GET;
    } else if (op_choice < this->get_ratio + this->put_ratio) {
        op_type = OpType::PUT;
    } else {
        op_type = OpType::DEL;
    }
    return op_type;
}

void KVWorkloadGenerator::next_operation(int tid, NextOperation &next_op)
{
    if (this->d_type != DynamismType::NONE) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        if (latency(this->last_interval, tv) >= this->d_interval) {
            this->last_interval = tv;
            change_keys();
        }
    }

    Operation &op = next_op.op;
    switch (this->key_type) {
    case KeyType::UNIFORM:
        op.key = this->keys->at(this->unif_int_dist.at(tid)(this->generator.at(tid)));
        break;
    case KeyType::ZIPF:
        op.key = this->keys->at(next_zipf_key_index(tid));
        break;
    }

    op.op_type = next_op_type(tid);
    if (op.op_type == OpType::PUT) {
        op.value = this->value;
    }

    switch (this->send_mode) {
    case SendMode::FIXED:
        break;
    case SendMode::DYNAMIC:
        adjust_send_rate(tid);
        break;
    }
    next_op.time = this->poisson_dist.at(tid)(this->generator.at(tid));
}

void KVWorkloadGenerator::change_keys()
{
    switch (this->d_type) {
    case DynamismType::HOTIN: {
        for (int i = 0; i < this->d_nkeys; i++) {
            this->keys->push_front(this->keys->back());
            this->keys->pop_back();
        }
        break;
    }
    case DynamismType::RANDOM: {
        for (int i = 0; i < this->d_nkeys; i++) {
            int k1 = rand() % 10000;
            int k2 = rand() % this->keys->size();
            std::string tmp = this->keys->at(k1);
            this->keys->at(k1) = this->keys->at(k2);
            this->keys->at(k2) = tmp;
        }
        break;
    }
    default:
        panic("Unknown dynamism type");
    }
}

void KVWorkloadGenerator::adjust_send_rate(int tid)
{
    if (++this->op_count[tid] >= LATENCY_CHECK_COUNT) {
        this->op_count[tid] = 0;
        if (this->stats->get_latency(tid, LATENCY_CHECK_PTILE) > this->target_latency) {
            this->mean_interval[tid] += 1;
        } else {
            this->mean_interval[tid] = std::max((long)MIN_INTERVAL, this->mean_interval[tid] - 1);
        }
        this->poisson_dist[tid] = std::poisson_distribution<long>(this->mean_interval[tid]);
    }
}

Client::Client(Configuration *config,
               MemcacheKVStats *stats,
               KVWorkloadGenerator *gen,
               MessageCodec *codec)
    : config(config), stats(stats), gen(gen), codec(codec)
{
}

Client::~Client()
{
}

void Client::receive_message(const Message &msg, const Address &addr, int tid)
{
    MemcacheKVMessage kvmsg;
    if (!this->codec->decode(msg, kvmsg)) {
        panic("Failed to decode message");
    }
    assert(kvmsg.type == MemcacheKVMessage::Type::REPLY);
    assert(kvmsg.reply.client_id == this->config->client_id);

    complete_op(tid, kvmsg.reply);
}

void Client::run()
{
    this->stats->start();
    this->transport->run_app_threads(this);
    this->stats->done();
    this->stats->dump();

}

void Client::run_thread(int tid)
{
    struct timeval start, now;
    struct timespec ts;
    gettimeofday(&start, nullptr);
    clock_gettime(CLOCK_REALTIME, &ts);
    NextOperation next_op;

    do {
        this->gen->next_operation(tid, next_op);
        wait_ns(ts, next_op.time);
        execute_op(next_op.op);
        this->stats->report_issue(tid);
        gettimeofday(&now, nullptr);
    } while (latency(start, now) < this->config->duration * 1000000);
}

void Client::execute_op(const Operation &op)
{
    struct timeval time;
    gettimeofday(&time, nullptr);

    MemcacheKVMessage kvmsg;
    kvmsg.type = MemcacheKVMessage::Type::REQUEST;
    kvmsg.request.client_id = this->config->client_id;
    kvmsg.request.server_id = key_to_node_id(op.key, this->config->num_nodes);
    kvmsg.request.req_id = req_id++;
    kvmsg.request.req_time = (uint32_t)time.tv_usec;
    kvmsg.request.op = op;

    Message msg;
    if (!this->codec->encode(msg, kvmsg)) {
        panic("Failed to encode message");
    }

    if (this->config->use_endhost_lb) {
        this->transport->send_message_to_lb(msg);
    } else {
        // Chain replication: send READs to tail rack and WRITEs to head rack
        int rack_id = op.op_type == OpType::GET ? this->config->num_racks-1 : 0;
        this->transport->send_message_to_node(msg, rack_id, kvmsg.request.server_id);
    }
}

void Client::complete_op(int tid, const MemcacheKVReply &reply)
{
    struct timeval start_time, end_time;
    gettimeofday(&end_time, nullptr);
    start_time.tv_sec = end_time.tv_sec;
    start_time.tv_usec = reply.req_time;
    if (end_time.tv_usec < start_time.tv_usec) {
        start_time.tv_sec -= 1; // assume request won't take > 1 sec
    }
    this->stats->report_op(tid,
                           reply.op_type,
                           latency(start_time, end_time),
                           reply.result == Result::OK);
}

} // namespace memcachekv
