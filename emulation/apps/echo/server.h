#ifndef _ECHO_SERVER_H_
#define _ECHO_SERVER_H_

#include <application.h>

namespace echo {

class Server : public Application {
public:
    Server();
    ~Server();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;
};

} // namespace echo

#endif /* _ECHO_SERVER_H_ */
