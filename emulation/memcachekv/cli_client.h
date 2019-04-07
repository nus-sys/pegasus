#ifndef __MEMCACHEKV_CLI_CLIENT_H__
#define __MEMCACHEKV_CLI_CLIENT_H__

#include "application.h"
#include "configuration.h"
#include "message.h"

namespace memcachekv {

class CLIClient : public Application {
public:
    CLIClient(Configuration *config,
           MessageCodec *codec,
           Operation op);
    ~CLIClient() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    Configuration *config;
    MessageCodec *codec;
    Operation op;
};

} // namespace memcachekv
#endif /* __MEMCACHEKV_CLI_CLIENT_H__ */
