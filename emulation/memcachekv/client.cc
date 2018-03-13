#include "memcachekv/client.h"
#include "memcachekv/memcachekv.pb.h"

using std::string;

namespace memcachekv {
using namespace proto;

Client::Client(Transport *transport, Configuration *config)
{
    this->transport = transport;
    this->config = config;
}

void
Client::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVReply reply;
    reply.ParseFromString(message);
}

void
Client::run()
{
    MemcacheKVRequest request;
    string request_str;
    request.set_req_id(1);
    request.mutable_operation()->set_op_type(Operation_Type_PUT);
    request.mutable_operation()->set_key("k1");
    request.mutable_operation()->set_value("v1");
    request.SerializeToString(&request_str);
    this->transport->send_message_to_node(request_str, 0);
}

} // namespace memcachekv
