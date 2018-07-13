#include "logger.h"
#include "memcachekv/migration_server.h"

using std::string;

namespace memcachekv {

void
MigrationServer::receive_message(const string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage request_msg;
    this->codec->decode(message, request_msg);
    switch (request_msg.type) {
    case MemcacheKVMessage::Type::MGR: {
        for (const auto &op : request_msg.migration_request.ops) {
            this->store[op.key] = op.value;
        }
        this->transport->send_message(string("DONE"), src_addr);
        break;
    }
    default:
        panic("Server received unexpected message");
    }
}

void
MigrationServer::run(int duration)
{
    // Do nothing...
}

} // namespace memcachekv
