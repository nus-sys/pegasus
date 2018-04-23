#include "memcachekv/controller.h"

namespace memcachekv {

Controller::Controller(Transport *transport,
                       Configuration *config,
                       const ControllerMessage &msg)
    : transport(transport), config(config), msg(msg) {}


void
Controller::receive_message(const std::string &message, const sockaddr &src_addr)
{
    ControllerMessage msg;
    if (!this->codec.decode(message, msg)) {
        return;
    }
    if (msg.type != ControllerMessage::Type::REPLY) {
        return;
    }
    if (msg.reply.ack == Ack::OK) {
        std::unique_lock<std::mutex> lck(mtx);
        this->replied = true;
        this->cv.notify_all();
    }
}

void
Controller::run(int duration)
{
    // Just send one controller message to the router
    this->replied = false;
    std::string msg_str;
    this->codec.encode(msg_str, this->msg);
    this->transport->send_message_to_addr(msg_str, this->config->controller_address);
    // Wait for ack from switch
    std::unique_lock<std::mutex> lck(mtx);
    while (!this->replied) {
        this->cv.wait(lck);
    }
}

} // namespace memcachekv
