#include "memcachekv/router.h"

namespace memcachekv {

void
Router::receive_message(const std::string &message, const sockaddr &src_addr)
{
    MemcacheKVMessage msg;
    this->codec->decode(message, msg);

    switch (msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        // request from client
        if (this->client_addresses.count(msg.request.client_id) == 0) {
            // Install client address
            this->client_addresses[msg.request.client_id] = src_addr;
        }
        // Forward to mapped kv server
        int node_id = this->config->key_to_node_id(msg.request.op.key);
        this->transport->send_message_to_addr(message, this->config->addresses[node_id]);
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        // reply from server
        // Forward to client
        this->transport->send_message(message, this->client_addresses.at(msg.reply.client_id));
        break;
    }
    default:
        printf("Unknown message type %u\n", static_cast<unsigned int>(msg.type));
    }
}

void
Router::run(int duration)
{
    // Do nothing
}

} // namespace memcachekv
