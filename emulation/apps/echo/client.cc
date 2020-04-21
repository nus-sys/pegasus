#include <logger.h>
#include <utils.h>
#include <apps/echo/client.h>

namespace echo {

#define RACK 0
#define NODE 0

Client::Client(Configuration *config, Stats *stats, int interval)
    : config(config), stats(stats), interval(interval)
{
}

Client::~Client()
{
}

void Client::receive_message(const Message &msg, const Address &addr)
{
    struct timeval now, sent;
    sent = *(struct timeval*)msg.buf();
    gettimeofday(&now, nullptr);
    this->stats->report_latency(latency(sent, now));
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
    char buf[1024];
    Message msg(buf, sizeof(struct timeval), false);
    struct timeval start;
    struct timeval *now = (struct timeval*)buf;
    gettimeofday(&start, nullptr);
    gettimeofday(now, nullptr);

    do {
        wait(*now, this->interval);
        this->transport->send_message_to_node(msg, RACK, NODE);
        this->stats->report_issue();
    } while (latency(start, *now) < this->config->duration * 1000000);
}

} // namespace echo
