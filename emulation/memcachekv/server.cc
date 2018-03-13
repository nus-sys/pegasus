#include "memcachekv/server.h"
#include "memcachekv/memcachekv.pb.h"

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
    reply.set_result(Result::OK);
    reply.set_value("");
    reply.SerializeToString(&reply_str);
    this->transport->send_message(reply_str, src_addr);
}

void
Server::run()
{
    // Do nothing...
}

} // namespace memcachekv
