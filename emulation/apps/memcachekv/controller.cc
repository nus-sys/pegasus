#include "memcachekv/controller.h"

namespace memcachekv {

Controller::Controller(Configuration *config,
                       const ControllerMessage &ctrlmsg)
    : config(config), ctrlmsg(ctrlmsg) {}


void Controller::receive_message(const Message &msg, const Address &addr, int tid)
{
    ControllerMessage ctrlmsg;
    if (!this->codec.decode(msg, ctrlmsg)) {
        return;
    }
    if (ctrlmsg.type != ControllerMessage::Type::RESET_REPLY) {
        return;
    }
    if (ctrlmsg.reset_reply.ack == Ack::OK) {
        std::unique_lock<std::mutex> lck(mtx);
        this->replied = true;
        this->cv.notify_all();
    }
}

void Controller::run()
{
    // Just send one message to the controller
    this->replied = false;
    Message msg;
    this->codec.encode(msg, this->ctrlmsg);
    for (int i = 0; i < this->config->num_racks; i++) {
        this->transport->send_message_to_controller(msg, i);
    }
    // Wait for ack
    /*
    std::unique_lock<std::mutex> lck(mtx);
    while (!this->replied) {
        this->cv.wait(lck);
    }
    */
}

void Controller::run_thread(int tid)
{
}

} // namespace memcachekv
