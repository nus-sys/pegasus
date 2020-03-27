#include "memcachekv/controller.h"

namespace memcachekv {

Controller::Controller(Configuration *config,
                       const ControllerMessage &msg)
    : config(config), msg(msg) {}


void Controller::receive_message(const std::string &message, const Address &addr)
{
    ControllerMessage msg;
    if (!this->codec.decode(message, msg)) {
        return;
    }
    if (msg.type != ControllerMessage::Type::RESET_REPLY) {
        return;
    }
    if (msg.reset_reply.ack == Ack::OK) {
        std::unique_lock<std::mutex> lck(mtx);
        this->replied = true;
        this->cv.notify_all();
    }
}

void Controller::run(int duration)
{
    // Just send one message to the controller
    this->replied = false;
    std::string msg_str;
    this->codec.encode(msg_str, this->msg);
    for (int i = 0; i < this->config->num_racks; i++) {
        this->transport->send_message_to_controller(msg_str, i);
    }
    // Wait for ack
    /*
    std::unique_lock<std::mutex> lck(mtx);
    while (!this->replied) {
        this->cv.wait(lck);
    }
    */
}

} // namespace memcachekv
