#include <logger.h>
#include <apps/echo/client.h>

namespace echo {

#define NMSGS 5
#define RACK 0
#define NODE 0

Client::Client()
    : received(false)
{
}

Client::~Client()
{
}

void Client::receive_message(const Message &msg, const Address &addr)
{
    std::unique_lock<std::mutex> lck(this->mtx);
    info("Received reply %s", msg.buf());
    this->received = true;
    this->cv.notify_all();
}

void Client::run()
{
    Message msg(std::string("echo"));
    std::unique_lock<std::mutex> lck(this->mtx);

    for (int i = 0; i < NMSGS; i++) {
        this->received = false;
        this->transport->send_message_to_node(msg, RACK, NODE);
        while (!this->received) {
            this->cv.wait(lck);
        }
    }
}

void Client::run_thread(int tid)
{
}

} // namespace echo
