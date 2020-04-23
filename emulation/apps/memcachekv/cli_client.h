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
                                 const Address &addr,
                                 int tid) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    Configuration *config;
    MessageCodec *codec;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_CLI_CLIENT_H_ */
