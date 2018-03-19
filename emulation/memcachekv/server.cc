#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage request_msg;
    this->codec->decode(message, request_msg);
    assert(request_msg.has_request);

    MemcacheKVMessage reply_msg;
    string reply_msg_str;
    reply_msg.has_request = false;
    reply_msg.has_reply = true;
    reply_msg.reply.client_id = request_msg.request.client_id;
    reply_msg.reply.req_id = request_msg.request.req_id;

    process_op(request_msg.request.op, reply_msg.reply);

    this->codec->encode(reply_msg_str, reply_msg);
    this->transport->send_message(reply_msg_str, src_addr);
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

} // namespace memcachekv
