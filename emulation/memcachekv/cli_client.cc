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
}

void
CLIClient::run(int duration)
{
    MemcacheKVMessage msg;
    string msg_str;
    msg.type = MemcacheKVMessage::Type::REQUEST;
    msg.request.client_id = 0;
    msg.request.req_id = 0;
    msg.request.node_id = this->config->key_to_node_id(op.key);
    msg.request.op = this->op;
    memset(&msg.request.client_addr, 0, sizeof(sockaddr));
    this->codec->encode(msg_str, msg);

    int rack_id = this->op.op_type == Operation::Type::GET ? this->config->num_racks-1 : 0;
    this->transport->send_message_to_node(msg_str, rack_id, msg.request.node_id);
}

} // namespace memcahcekv
