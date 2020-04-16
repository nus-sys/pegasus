#include <iostream>
#include <cassert>
#include <unistd.h>

#include <apps/memcachekv/cli_client.h>
#include <apps/memcachekv/utils.h>

using std::string;
using std::cin;

namespace memcachekv {

CLIClient::CLIClient(Configuration *config,
                     MessageCodec *codec)
    : config(config), codec(codec)
{
}

CLIClient::~CLIClient()
{
}

void CLIClient::receive_message(const Message &msg, const Address &addr)
{
    MemcacheKVMessage kvmsg;
    this->codec->decode(msg, kvmsg);
    assert(kvmsg.type == MemcacheKVMessage::Type::REPLY);
    printf("Reply type %u keyhash %u node %u ver %u result %u value %s\n",
           (unsigned int)kvmsg.reply.type, kvmsg.reply.keyhash, kvmsg.reply.node_id,
           kvmsg.reply.ver, (unsigned int)kvmsg.reply.result, kvmsg.reply.value.c_str());
}

void CLIClient::run()
{
    MemcacheKVMessage kvmsg;
    string input;

    kvmsg.type = MemcacheKVMessage::Type::REQUEST;
    kvmsg.request.client_id = 0;
    kvmsg.request.req_id = 0;
    while (true) {
        printf("op type (0-read, 1-write): ");
        getline(cin, input);
        kvmsg.request.op.op_type = static_cast<Operation::Type>(stoi(input));
        printf("key: ");
        getline(cin, input);
        kvmsg.request.op.key = input;
        printf("value: ");
        getline(cin, input);
        kvmsg.request.op.value = input;
        kvmsg.request.req_id++;
        kvmsg.request.node_id = key_to_node_id(kvmsg.request.op.key,
                                               this->config->num_nodes);

        Message msg;
        this->codec->encode(msg, kvmsg);

        int rack_id = kvmsg.request.op.op_type == Operation::Type::GET ? this->config->num_racks-1 : 0;
        this->transport->send_message_to_node(msg, rack_id, kvmsg.request.node_id);
        sleep(1);
    }
}

void CLIClient::run_thread(int tid)
{
}

} // namespace memcahcekv
