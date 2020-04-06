#ifndef _MEMCACHEKV_CLI_CLIENT_H_
#define _MEMCACHEKV_CLI_CLIENT_H_

#include <application.h>
#include <configuration.h>
#include <message.h>

namespace memcachekv {

class CLIClient : public Application {
public:
    CLIClient(Configuration *config,
              MessageCodec *codec);
    ~CLIClient();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run(int duration) override final;

private:
    Configuration *config;
    MessageCodec *codec;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_CLI_CLIENT_H_ */
