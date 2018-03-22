#ifndef __MEMCACHEKV_CONTROLLER_H__
#define __MEMCACHEKV_CONTROLLER_H__

#include "application.h"
#include "configuration.h"
#include "memcachekv/message.h"

namespace memcachekv {

class Controller : public Application {
public:
    Controller(Transport *transport,
               Configuration *config,
               const ControllerMessage &msg);
    ~Controller() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    Transport *transport;
    Configuration *config;
    ControllerCodec codec;
    ControllerMessage msg;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_CONTROLLER_H__ */
