#include "logger.h"
#include "utils.h"
#include "memcachekv/config.h"
#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

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
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
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
    reply_msg.reply.client_id = msg.client_id;
    reply_msg.reply.req_id = msg.req_id;

    process_op(msg.op, reply_msg.reply);

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
    case Operation::Type::GETM: {
        // XXX todo
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
}

} // namespace memcachekv
