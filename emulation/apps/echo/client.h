#ifndef _ECHO_CLIENT_H_
#define _ECHO_CLIENT_H_

#include <mutex>
#include <condition_variable>

#include <application.h>
#include <configuration.h>

namespace echo {

class Client : public Application {
public:
    Client();
    ~Client();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run(int duration) override final;

private:
    std::mutex mtx;
    std::condition_variable cv;
    bool received;
};

} // namespace echo

#endif /* _ECHO_CLIENT_H_ */
