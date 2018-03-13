#include "memcachekv/server.h"

using namespace std;

namespace memcachekv {

Server::Server(Transport *transport, Configuration *config)
{
    this->transport = transport;
    this->config = config;
}

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    printf("Server: received msg %s\n", message.c_str());
    this->transport->send_message(string("ACK!"), src_addr);
}

void
Server::run()
{
    // Do nothing...
}

} // namespace memcachekv
