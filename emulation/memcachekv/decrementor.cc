#include "decrementor.h"
#include "utils.h"

namespace memcachekv {

static void convert_endian(void *dst, const void *src, size_t size)
{
    uint8_t *dptr, *sptr;
    for (dptr = (uint8_t*)dst, sptr = (uint8_t*)src + size - 1;
         size > 0;
         size--) {
        *dptr++ = *sptr--;
    }
}

Decrementor::Decrementor(Transport *transport, Configuration *config,
                         int interval, int n_dec)
    : transport(transport), config(config),
    interval(interval), n_dec(n_dec)
{
}

void
Decrementor::receive_message(const std::string &message, const sockaddr &src_addr)
{
    // Should never receive messages
    return;
}

void
Decrementor::run(int duration)
{
    struct timeval prev, now;
    char buf[BUFSIZE];
    memset(buf, 0, BUFSIZE);
    char *ptr = buf;
    *(identifier_t*)ptr = PEGASUS;
    ptr += sizeof(identifier_t);
    *(op_type_t*)ptr = DEC;
    ptr += sizeof(op_type_t);
    ptr += sizeof(keyhash_t);
    ptr += sizeof(node_t);
    convert_endian(ptr, &this->n_dec, sizeof(load_t));
    ptr -= sizeof(node_t);

    std::string msgs[this->config->num_nodes/2];
    for (int i = 0; i < this->config->num_nodes/2; i++) {
        *(node_t*)ptr = i * 2;
        msgs[i] = std::string(buf, BUFSIZE);
    }

    gettimeofday(&prev, nullptr);
    now = prev;
    while (true) {
        while (latency(prev, now) < this->interval) {
            gettimeofday(&now, nullptr);
        }
        prev = now;
        for (int i = 0; i < this->config->num_nodes/2; i++) {
            this->transport->send_message_to_node(msgs[i], 0);
        }
    }
}

} // namespace memcachekv
