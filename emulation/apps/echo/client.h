#ifndef _ECHO_CLIENT_H_
#define _ECHO_CLIENT_H_

#include <mutex>
#include <condition_variable>

#include <application.h>
#include <stats.h>

namespace echo {

class Client : public Application {
public:
    Client(Configuration *config, Stats *stats, int interval);
    ~Client();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    Configuration *config;
    Stats *stats;
    int interval;
};

} // namespace echo

#endif /* _ECHO_CLIENT_H_ */
