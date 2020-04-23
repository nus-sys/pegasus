#ifndef _MEMCACHEKV_CONTROLLER_H_
#define _MEMCACHEKV_CONTROLLER_H_

#include <mutex>
#include <condition_variable>
#include "application.h"
#include "configuration.h"
#include "memcachekv/message.h"

namespace memcachekv {

class Controller : public Application {
public:
    Controller(Configuration *config,
               const ControllerMessage &msg);
    ~Controller() {};

    virtual void receive_message(const Message &msg,
                                 const Address &addr,
                                 int tid) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    Configuration *config;
    ControllerCodec codec;
    ControllerMessage ctrlmsg;
    std::mutex mtx;
    std::condition_variable cv;
    bool replied;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_CONTROLLER_H_ */
