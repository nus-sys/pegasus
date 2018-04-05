#ifndef __MEMCACHEKV_MIGRATION_SERVER_H__
#define __MEMCACHEKV_MIGRATION_SERVER_H__

#include <unordered_map>
#include "application.h"
#include "memcachekv/message.h"

namespace memcachekv {

class MigrationServer : public Application {
public:
    MigrationServer(Transport *transport,
                    Configuration *config,
                    MessageCodec *codec)
        : transport(transport),
        config(config),
        codec(codec) {};
    ~MigrationServer() {};

    void receive_message(const std::string &message,
                         const sockaddr &src_addr) override;
    void run(int duration) override;

private:
    Transport *transport;
    Configuration *config;
    MessageCodec *codec;
    std::unordered_map<std::string, std::string> store;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MIGRATION_SERVER_H__ */
