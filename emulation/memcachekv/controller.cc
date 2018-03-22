#include "memcachekv/controller.h"

namespace memcachekv {

Controller::Controller(Transport *transport,
                       Configuration *config,
                       const ControllerMessage &msg)
    : transport(transport), config(config), msg(msg) {}


void
Controller::receive_message(const std::string &message, const sockaddr &src_addr)
{
    // Do nothing
}

void
Controller::run(int duration)
{
    // Just send one controller message to the router
    std::string msg_str;
    this->codec.encode(msg_str, this->msg);
    this->transport->send_message_to_addr(msg_str, this->config->router_address);
}

} // namespace memcachekv
