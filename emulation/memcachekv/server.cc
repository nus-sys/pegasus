#include <functional>
#include <set>
#include "logger.h"
#include "utils.h"
#include "memcachekv/config.h"
#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

Server::Server(Transport *transport, Configuration *config, MessageCodec *codec,
       ControllerCodec *ctrl_codec, int node_id, int proc_latency)
    : transport(transport),
    config(config),
    codec(codec),
    ctrl_codec(ctrl_codec),
    node_id(node_id),
    proc_latency(proc_latency)
{
    this->epoch_start.tv_sec = 0;
    this->epoch_start.tv_usec = 0;
    this->request_count = 0;
}

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    // Check for controller message
    ControllerMessage ctrl_msg;
    if (this->ctrl_codec->decode(message, ctrl_msg)) {
        process_ctrl_message(ctrl_msg, src_addr);
        return;
    }

    // KV message
    MemcacheKVMessage kv_msg;
    if (this->codec->decode(message, kv_msg)) {
        process_kv_message(kv_msg, src_addr);
        return;
    }
}

typedef std::function<bool(std::pair<keyhash_t, unsigned int>,
                           std::pair<keyhash_t, unsigned int>)> Comparator;
static Comparator comp =
[](std::pair<keyhash_t, unsigned int> a,
   std::pair<keyhash_t, unsigned int> b)
{
    return a.second > b.second;
};

void
Server::run(int duration)
{
    // Send HK report periodically
    while (true) {
        usleep(HK_EPOCH);
        // Sort hk_report
        this->hk_mutex.lock();
        std::set<std::pair<keyhash_t, unsigned int>, Comparator> sorted_hk(this->hk_report.begin(), this->hk_report.end(), comp);
        this->hk_report.clear();
        this->key_count.clear();
        this->hk_mutex.unlock();

        if (sorted_hk.size() == 0) {
            continue;
        }

        // hk report has a max of MAX_HK_SIZE reports
        ControllerMessage msg;
        string msg_str;
        msg.type = ControllerMessage::Type::HK_REPORT;
        int i = 0;
        for (const auto &hk : sorted_hk) {
            if (i >= MAX_HK_SIZE) {
                break;
            }
            msg.hk_report.reports.push_back(ControllerHKReport::Report(hk.first, hk.second));
            i++;
        }
        if (this->ctrl_codec->encode(msg_str, msg)) {
            this->transport->send_message_to_controller(msg_str);
        } else {
            printf("Failed to encode hk report\n");
        }
    }
}

void
Server::process_kv_message(const MemcacheKVMessage &msg,
                           const sockaddr &addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    switch (msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        process_kv_request(msg.request, addr);
        break;
    }
    case MemcacheKVMessage::Type::MGR: {
        process_kv_migration(msg.migration_request, addr);
        break;
    }
    default:
        panic("Server received unexpected kv message");
    }
}

void
Server::process_ctrl_message(const ControllerMessage &msg,
                             const sockaddr &addr)
{
    switch (msg.type) {
    default:
        panic("Server received unexpected controller message");
    }
}

void
Server::process_kv_request(const MemcacheKVRequest &msg,
                           const sockaddr &addr)
{
    MemcacheKVMessage reply_msg;
    string reply_msg_str;
    reply_msg.type = MemcacheKVMessage::Type::REPLY;
    reply_msg.reply.node_id = this->node_id;
    reply_msg.reply.client_id = msg.client_id;
    reply_msg.reply.req_id = msg.req_id;

    process_op(msg.op, reply_msg.reply);
    reply_msg.reply.load = calculate_load();

    this->codec->encode(reply_msg_str, reply_msg);
    this->transport->send_message(reply_msg_str, addr);
}

void
Server::process_kv_migration(const MigrationRequest &msg,
                             const sockaddr &addr)
{
    for (const auto &op : msg.ops) {
        this->store[op.key] = op.value;
    }
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    switch (op.op_type) {
    case Operation::Type::GET: {
        if (this->store.count(op.key) > 0) {
            // Key is present
            reply.result = Result::OK;
            reply.value = this->store.at(op.key);
        } else {
            // Key not found
            reply.result = Result::NOT_FOUND;
            reply.value = "";
        }
        break;
    }
    case Operation::Type::PUT: {
        this->store[op.key] = op.value;
        reply.result = Result::OK;
        reply.value = "";
        break;
    }
    case Operation::Type::DEL: {
        this->store.erase(op.key);
        reply.result = Result::OK;
        reply.value = "";
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
    update_rate(op);
}

void
Server::update_rate(const Operation &op)
{
    if (++this->request_count % KR_SAMPLE_RATE == 0) {
        this->hk_mutex.lock();
        if (++this->key_count[op.keyhash] >= this->HK_THRESHOLD) {
            this->hk_report[op.keyhash] = this->key_count[op.keyhash];
        }
        this->hk_mutex.unlock();
    }
}

load_t
Server::calculate_load()
{
    struct timeval now;

    gettimeofday(&now, nullptr);
    if (this->epoch_start.tv_sec == 0 && this->epoch_start.tv_usec == 0) {
        // Initialize epoch
        this->epoch_start = now;
    }
    this->request_ts.push_back(now);

    if (latency(this->epoch_start, now) > EPOCH_DURATION) {
        this->epoch_start = get_prev_timeval(now, EPOCH_DURATION);
        for (auto it = this->request_ts.begin(); it != this->request_ts.end(); ) {
            // Remove ts that fall out of the epoch
            if (timeval_cmp(*it, this->epoch_start) < 0) {
                it = this->request_ts.erase(it);
            } else {
                // ts should be in order
                break;
            }
        }
    }

    return this->request_ts.size();
}

} // namespace memcachekv
