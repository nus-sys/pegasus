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
    }
}

void
Server::run(int duration)
{
    // Empty
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
        }
        break;
    }
    case Operation::Type::PUT: {
        this->store[op.key] = op.value;
        reply.result = Result::OK;
        break;
    }
    case Operation::Type::DEL: {
        this->store.erase(op.key);
        reply.result = Result::OK;
        break;
    }
    default:
        panic("Unknown memcachekv op type");
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
