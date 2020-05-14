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

KVWorkloadGenerator::ThreadState::ThreadState()
    : op_count(0), mean_interval(0)
{
}

KVWorkloadGenerator::KVWorkloadGenerator(std::deque<std::string> &keys,
                                         int value_len,
                                         float get_ratio,
                                         float put_ratio,
                                         float mean_interval,
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
        for (unsigned int i = 0; i < keys.size(); i++) {
            c = c + (1.0 / pow((float)(i+1), alpha));
        }
        c = 1 / c;
        float sum = 0;
        for (unsigned int i = 0; i < keys.size(); i++) {
            sum += (c / pow((float)(i+1), alpha));
            this->zipfs.push_back(sum);
        }
    }
    struct timeval time;
    gettimeofday(&time, nullptr);
    this->last_interval = time;

    // Per thread initialization
    for (int i = 0; i < n_threads; i++) {
        ThreadState ts;
        ts.mean_interval = (long)mean_interval;
        ts.generator = std::default_random_engine(time.tv_usec + i);
        ts.unif_real_dist = std::uniform_real_distribution<float>(0.0, 1.0);
        ts.unif_int_dist = std::uniform_int_distribution<int>(0, keys.size()-1);
        ts.poisson_dist = std::poisson_distribution<long>((long)mean_interval);
        this->thread_states.push_back(ts);
    }
}

KVWorkloadGenerator::~KVWorkloadGenerator()
{
}

int KVWorkloadGenerator::next_zipf_key_index(int tid)
{
    ThreadState &ts = this->thread_states.at(tid);
    float random = 0.0;
    while (random == 0.0) {
        random = ts.unif_real_dist(ts.generator);
    }

    int l = 0, r = this->keys.size(), mid = 0;
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
    ThreadState &ts = this->thread_states.at(tid);
    float op_choice = ts.unif_real_dist(ts.generator);
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

void KVWorkloadGenerator::next_operation(int tid, Operation &op, long &time)
{
    if (this->d_type != DynamismType::NONE) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        if (latency(this->last_interval, tv) >= this->d_interval) {
            this->last_interval = tv;
            change_keys();
        }
    }

    ThreadState &ts = this->thread_states.at(tid);
    switch (this->key_type) {
    case KeyType::UNIFORM:
        op.key = this->keys.at(ts.unif_int_dist(ts.generator));
        break;
    case KeyType::ZIPF:
        op.key = this->keys.at(next_zipf_key_index(tid));
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
    time = ts.poisson_dist(ts.generator);
}

void KVWorkloadGenerator::change_keys()
{
    switch (this->d_type) {
    case DynamismType::HOTIN: {
        for (int i = 0; i < this->d_nkeys; i++) {
            this->keys.push_front(this->keys.back());
            this->keys.pop_back();
        }
        break;
    }
    case DynamismType::RANDOM: {
        for (int i = 0; i < this->d_nkeys; i++) {
            int k1 = rand() % 10000;
            int k2 = rand() % this->keys.size();
            std::string tmp = this->keys.at(k1);
            this->keys.at(k1) = this->keys.at(k2);
            this->keys.at(k2) = tmp;
        }
        break;
    }
    default:
        panic("Unknown dynamism type");
    }
}

void KVWorkloadGenerator::adjust_send_rate(int tid)
{
    ThreadState &ts = this->thread_states.at(tid);
    if (++ts.op_count >= LATENCY_CHECK_COUNT) {
        ts.op_count = 0;
        if (this->stats->get_latency(tid, LATENCY_CHECK_PTILE) > this->target_latency) {
            ts.mean_interval += 1;
        } else {
            ts.mean_interval = std::max((long)MIN_INTERVAL, ts.mean_interval - 1);
        }
        ts.poisson_dist = std::poisson_distribution<long>(ts.mean_interval);
    }
}

Client::Client(Configuration *config,
               Stats *stats,
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
    gettimeofday(&start, nullptr);
    gettimeofday(&now, nullptr);
    MemcacheKVMessage msg;
    msg.type = MemcacheKVMessage::Type::REQUEST;
    msg.request.client_id = this->config->client_id;
    long time;

    do {
        this->gen->next_operation(tid, msg.request.op, time);
        wait_ticks(time);
        gettimeofday(&now, nullptr);
        msg.request.req_time = (uint32_t)now.tv_usec;
        msg.request.server_id = key_to_node_id(msg.request.op.key, this->config->num_nodes);
        msg.request.req_id = req_id++;
        execute_op(msg);
        this->stats->report_issue(tid);
    } while (latency(start, now) < this->config->duration * 1000000);
}

void Client::execute_op(const MemcacheKVMessage &kvmsg)
{
    Message msg;
    if (!this->codec->encode(msg, kvmsg)) {
        panic("Failed to encode message");
    }

    if (this->config->use_endhost_lb) {
        this->transport->send_message_to_lb(msg);
    } else {
        // Chain replication: send READs to tail rack and WRITEs to head rack
        int rack_id = kvmsg.request.op.op_type == OpType::GET ? this->config->num_racks-1 : 0;
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
    this->stats->report_latency(tid, latency(start_time, end_time));
}

} // namespace memcachekv
