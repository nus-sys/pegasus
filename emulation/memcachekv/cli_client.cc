#include <iostream>
#include <unistd.h>
#include "memcachekv/cli_client.h"

using std::string;
using std::cin;

namespace memcachekv {

CLIClient::CLIClient(Configuration *config,
                     MessageCodec *codec)
    : config(config), codec(codec)
{
}

void
CLIClient::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage msg;
    this->codec->decode(message, msg);
    assert(msg.type == MemcacheKVMessage::Type::REPLY);
    printf("Reply type %u keyhash %u node %u ver %u result %u value %s\n",
           (unsigned int)msg.reply.type, msg.reply.keyhash, msg.reply.node_id,
           msg.reply.ver, (unsigned int)msg.reply.result, msg.reply.value.c_str());
}

void
CLIClient::run(int duration)
{
    MemcacheKVMessage msg;
    string input, msg_str;

    msg.type = MemcacheKVMessage::Type::REQUEST;
    msg.request.client_id = 0;
    msg.request.req_id = 0;
    memset(&msg.request.client_addr, 0, sizeof(sockaddr));
    while (true) {
        printf("op type (0-read, 1-write): ");
        getline(cin, input);
        msg.request.op.op_type = static_cast<Operation::Type>(stoi(input));
        printf("key: ");
        getline(cin, input);
        msg.request.op.key = input;
        printf("value: ");
        getline(cin, input);
        msg.request.op.value = input;
        msg.request.req_id++;
        msg.request.node_id = this->config->key_to_node_id(msg.request.op.key);
        this->codec->encode(msg_str, msg);

        int rack_id = msg.request.op.op_type == Operation::Type::GET ? this->config->num_racks-1 : 0;
        this->transport->send_message_to_node(msg_str, rack_id, msg.request.node_id);
        sleep(1);
    }
}

} // namespace memcahcekv
