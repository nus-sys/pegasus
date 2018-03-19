#include "memcachekv/router.h"

namespace memcachekv {
using namespace proto;

void
Router::receive_message(const std::string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage msg;
    msg.ParseFromString(message);

    if (msg.has_request()) {
        // request from client
        if (this->client_addresses.count(msg.request().client_id()) == 0) {
            // Install client address
            this->client_addresses[msg.request().client_id()] = src_addr;
        }
        // Forward to mapped kv server
        const NodeAddress& addr = this->config->key_to_address(msg.request().op().key());
        this->transport->send_message_to_addr(message, addr);
    } else if (msg.has_reply()) {
        // reply from server
        // Forward to client
        this->transport->send_message(message, this->client_addresses.at(msg.reply().client_id()));
    }
}

void
Router::run(int duration)
{
    // Do nothing
}

} // namespace memcachekv
