#include <cassert>
#include <sys/time.h>
#include <thread>

#include <utils.h>
#include <logger.h>
#include <apps/memcachekv/client.h>
#include <apps/memcachekv/utils.h>

#define LATENCY_CHECK_COUNT 100
#define LATENCY_CHECK_PTILE 0.99
#define MIN_INTERVAL 1

using std::string;

namespace memcachekv {

KVWorkloadGenerator::KVWorkloadGenerator(std::deque<std::string> *keys,
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
        this->poisson_dist.push_back(std::poisson_distribution<int>(mean_interval));
    }
}

int KVWorkloadGenerator::next_zipf_key_index(int tid)
{
    float random = 0.0;
    while (random == 0.0) {
        random = this->unif_real_dist[tid](this->generator[tid]);
    }

    int l = 0, r = this->keys->size(), mid = 0;
    while (l < r) {
        mid = (l + r) / 2;
        if (random > this->zipfs[mid]) {
            l = mid + 1;
        } else if (random < this->zipfs[mid]) {
            r = mid - 1;
        } else {
            break;
        }
    }
    return mid;
}

Operation::Type KVWorkloadGenerator::next_op_type(int tid)
{
    float op_choice = this->unif_real_dist[tid](this->generator[tid]);
    Operation::Type op_type;
    if (op_choice < this->get_ratio) {
        op_type = Operation::Type::GET;
    } else if (op_choice < this->get_ratio + this->put_ratio) {
        op_type = Operation::Type::PUT;
    } else {
        op_type = Operation::Type::DEL;
    }
    return op_type;
}

const NextOperation &KVWorkloadGenerator::next_operation(int tid)
{
    if (this->d_type != DynamismType::NONE) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        if (latency(this->last_interval, tv) >= this->d_interval) {
            this->last_interval = tv;
            change_keys();
        }
    }

    Operation &op = this->next_op.op;
    switch (this->key_type) {
    case KeyType::UNIFORM:
        op.key = this->keys->at(this->unif_int_dist[tid](this->generator[tid]));
        break;
    case KeyType::ZIPF:
        op.key = this->keys->at(next_zipf_key_index(tid));
        break;
    }

    op.op_type = next_op_type(tid);
    if (op.op_type == Operation::Type::PUT) {
        op.value = this->value;
    }

    switch (this->send_mode) {
    case SendMode::FIXED:
        break;
    case SendMode::DYNAMIC:
        adjust_send_rate(tid);
        break;
    }
    this->next_op.time = this->poisson_dist[tid](this->generator[tid]);
    return this->next_op;
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
        if (this->stats->get_latency(LATENCY_CHECK_PTILE) > this->target_latency) {
            this->mean_interval[tid] += 1;
        } else {
            this->mean_interval[tid] = std::max(MIN_INTERVAL, this->mean_interval[tid] - 1);
        }
        this->poisson_dist[tid] = std::poisson_distribution<int>(this->mean_interval[tid]);
    }
}

Client::Client(Configuration *config,
               MemcacheKVStats *stats,
               KVWorkloadGenerator *gen,
               MessageCodec *codec)
    : config(config), stats(stats), gen(gen), codec(codec), req_id(1)
{
}

Client::~Client()
{
}

void Client::receive_message(const Message &msg, const Address &addr)
{
    MemcacheKVMessage kvmsg;
    this->codec->decode(msg, kvmsg);
    assert(kvmsg.type == MemcacheKVMessage::Type::REPLY);
    assert(kvmsg.reply.client_id == this->config->client_id);
    PendingRequest &pending_request = get_pending_request(kvmsg.reply.req_id);

    if (pending_request.op_type == Operation::Type::GET) {
        complete_op(kvmsg.reply.req_id, pending_request, kvmsg.reply.result);
    } else {
        pending_request.received_acks += 1;
        if (pending_request.received_acks >= pending_request.expected_acks) {
            complete_op(kvmsg.reply.req_id, pending_request, kvmsg.reply.result);
        }
    }
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

    do {
        const NextOperation &next_op = this->gen->next_operation(tid);
        wait(now, next_op.time);
        execute_op(next_op.op);
    } while (latency(start, now) < this->config->duration * 1000000);
}

void Client::execute_op(const Operation &op)
{
    PendingRequest pending_request;
    gettimeofday(&pending_request.start_time, nullptr);
    pending_request.op_type = op.op_type;
    pending_request.expected_acks = 1;
    uint32_t req_id = std::atomic_fetch_add(&this->req_id, {1});
    insert_pending_request(req_id, pending_request);

    MemcacheKVMessage kvmsg;
    kvmsg.type = MemcacheKVMessage::Type::REQUEST;
    kvmsg.request.client_id = this->config->client_id;
    kvmsg.request.req_id = req_id;
    kvmsg.request.node_id = key_to_node_id(op.key, this->config->num_nodes);
    kvmsg.request.op = op;

    Message msg;
    this->codec->encode(msg, kvmsg);

    // Chain replication: send READs to tail rack and WRITEs to head rack
    int rack_id = op.op_type == Operation::Type::GET ? this->config->num_racks-1 : 0;
    this->transport->send_message_to_node(msg, rack_id, kvmsg.request.node_id);

    this->stats->report_issue();
}

void Client::complete_op(uint32_t req_id, const PendingRequest &request, Result result)
{
    struct timeval end_time;
    gettimeofday(&end_time, nullptr);
    this->stats->report_op(request.op_type,
                           latency(request.start_time, end_time),
                           result == Result::OK);
    delete_pending_request(req_id);
}

void Client::insert_pending_request(uint32_t req_id, const PendingRequest &request)
{
    std::lock_guard<std::mutex> lck(this->pending_requests_mutex);
    this->pending_requests[req_id] = request;
}

PendingRequest& Client::get_pending_request(uint32_t req_id)
{
    std::lock_guard<std::mutex> lck(this->pending_requests_mutex);
    assert(this->pending_requests.count(req_id) > 0);
    return this->pending_requests.at(req_id);
}

void Client::delete_pending_request(uint32_t req_id)
{
    std::lock_guard<std::mutex> lck(this->pending_requests_mutex);
    this->pending_requests.erase(req_id);
}

} // namespace memcachekv
