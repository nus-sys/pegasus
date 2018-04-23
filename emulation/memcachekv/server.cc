#include "logger.h"
#include "memcachekv/server.h"
#include "utils.h"

using std::string;

namespace memcachekv {

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    MemcacheKVMessage request_msg;
    this->codec->decode(message, request_msg);
    switch (request_msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        MemcacheKVMessage reply_msg;
        string reply_msg_str;
        reply_msg.type = MemcacheKVMessage::Type::REPLY;
        reply_msg.reply.client_id = request_msg.request.client_id;
        reply_msg.reply.req_id = request_msg.request.req_id;

        process_op(request_msg.request.op, reply_msg.reply);

        this->codec->encode(reply_msg_str, reply_msg);
        this->transport->send_message(reply_msg_str, src_addr);
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        for (const auto &op : request_msg.migration_request.ops) {
            this->store[op.key] = op.value;
        }
        break;
    }
    default:
        panic("Server received unexpected message");
    }
}

void
Server::run(int duration)
{
    // Do nothing...
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
        printf("Unknown memcachekv op type %d\n", op.op_type);
    }
}

void
Server::migrate_key_to_node(const std::string &key, int node_id)
{
    MemcacheKVMessage msg;
    std::string msg_str;
    msg.type = MemcacheKVMessage::Type::MIGRATION_REQUEST;
    msg.migration_request.ops.push_back(Operation());
    Operation &op = msg.migration_request.ops.back();
    op.op_type = Operation::Type::PUT;
    op.key = key;
    if (this->store.count(key) > 0) {
        op.value = this->store.at(key);
    } else {
        op.value = string("");
    }

    this->codec->encode(msg_str, msg);
    this->transport->send_message_to_node(msg_str, node_id);
}

} // namespace memcachekv
