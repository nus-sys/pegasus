#include "memcachekv/cli_client.h"

using std::string;

namespace memcachekv {

CLIClient::CLIClient(Configuration *config,
                     MessageCodec *codec,
                     Operation op)
    : config(config), codec(codec), op(op)
{
}

void
CLIClient::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage msg;
    this->codec->decode(message, msg);
    assert(msg.type == MemcacheKVMessage::Type::REPLY);
    printf("Received reply type %u keyhash %u node %u ver %u result %u value %s\n",
           (unsigned int)msg.reply.type, msg.reply.keyhash, msg.reply.node_id,
           msg.reply.ver, (unsigned int)msg.reply.result, msg.reply.value.c_str());
    this->transport->stop();
}

void
CLIClient::run(int duration)
{
    MemcacheKVMessage msg;
    string msg_str;
    msg.type = MemcacheKVMessage::Type::REQUEST;
    msg.request.client_id = 0;
    msg.request.req_id = 0;
    int node_id = this->config->key_to_node_id(op.key);
    this->op.node_id = node_id;
    msg.request.op = this->op;
    this->codec->encode(msg_str, msg);

    this->transport->send_message_to_addr(msg_str, this->config->addresses[node_id]);
}

} // namespace memcahcekv
