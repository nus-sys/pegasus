#ifndef __MEMCACHEKV_CONTROLLER_H__
#define __MEMCACHEKV_CONTROLLER_H__

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

    virtual void receive_message(const std::string &message,
                                 const Address &addr) override final;
    virtual void run(int duration) override;

private:
    Configuration *config;
    ControllerCodec codec;
    ControllerMessage msg;
    std::mutex mtx;
    std::condition_variable cv;
    bool replied;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CONTROLLER_H__ */
