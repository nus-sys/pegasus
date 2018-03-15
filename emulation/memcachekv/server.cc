#include "memcachekv/server.h"

using std::string;

namespace memcachekv {
using namespace proto;

Server::Server(Transport *transport, Configuration *config)
{
    this->transport = transport;
    this->config = config;
}

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVRequest request;
    request.ParseFromString(message);

    MemcacheKVReply reply;
    string reply_str;
    reply.set_req_id(request.req_id());

    process_op(request.op(), reply);

    reply.SerializeToString(&reply_str);
    this->transport->send_message(reply_str, src_addr);
}

void
Server::run(int duration)
{
    // Do nothing...
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    switch (op.op_type()) {
    case Operation_Type_GET: {
        if (this->store.count(op.key()) > 0) {
            // Key is present
            reply.set_result(Result::OK);
            reply.set_value(this->store.at(op.key()));
        } else {
            // Key not found
            reply.set_result(Result::NOT_FOUND);
        }
        break;
    }
    case Operation_Type_PUT: {
        this->store[op.key()] = op.value();
        reply.set_result(Result::OK);
        break;
    }
    case Operation_Type_DEL: {
        this->store.erase(op.key());
        reply.set_result(Result::OK);
        break;
    }
    default:
        printf("Unknown memcachekv op type %d\n", op.op_type());
    }
}

} // namespace memcachekv
