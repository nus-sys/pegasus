#include <apps/echo/server.h>

namespace echo {

Server::Server()
{
}

Server::~Server()
{
}

void Server::receive_message(const Message &msg, const Address &addr)
{
    this->transport->send_message(msg, addr);
}

void Server::run()
{
    // Do nothing
}

void Server::run_thread(int tid)
{
}

} // namespace cho
