#include <logger.h>
#include <utils.h>
#include <apps/echo/client.h>

namespace echo {

#define RACK 0
#define NODE 0

Client::Client(Configuration *config, Stats *stats, float interval, int msglen)
    : config(config), stats(stats), interval((int)interval), msglen(msglen)
{
}

Client::~Client()
{
}

void Client::receive_message(const Message &msg, const Address &addr, int tid)
{
    struct timeval now, sent;
    sent = *(struct timeval*)msg.buf();
    gettimeofday(&now, nullptr);
    this->stats->report_latency(tid, latency(sent, now));
}

void Client::run()
{
    this->stats->start();
    this->transport->run_app_threads(this);
    this->stats->done();
    this->stats->dump();
}

void Client::run_thread(int tid)
{
    char buf[this->msglen];
    Message msg(buf, this->msglen, false);
    struct timeval start;
    struct timeval *now = (struct timeval*)buf;
    gettimeofday(&start, nullptr);
    gettimeofday(now, nullptr);

    do {
        wait(*now, this->interval);
        this->transport->send_message_to_node(msg, RACK, NODE);
        this->stats->report_issue(tid);
    } while (latency(start, *now) < this->config->duration * 1000000);
}

} // namespace echo
