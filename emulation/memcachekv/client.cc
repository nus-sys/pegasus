#include "memcachekv/client.h"

using namespace std;

namespace memcachekv {

Client::Client(Transport *transport, Configuration *config)
{
    this->transport = transport;
    this->config = config;
}

void
Client::receive_message(const string &message, const sockaddr &src_addr)
{
    printf("Client: received msg %s\n", message.c_str());
}

void
Client::run()
{
    this->transport->send_message_to_node(string("Hello!"), 0);
}

} // namespace memcachekv
