#include <cstring>

#include <utils.h>
#include <apps/memcachekv/decrementor.h>

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

Decrementor::Decrementor(Configuration *config, int interval, int n_dec)
    : config(config), interval(interval), n_dec(n_dec)
{
}

Decrementor::~Decrementor()
{
}

void
Decrementor::receive_message(const Message &msg, const Address &addr, int tid)
{
    // Should never receive messages
    return;
}

void
Decrementor::run()
{
    this->transport->run_app_threads(this);
}

void Decrementor::run_thread(int tid)
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

    Message msgs[this->config->num_nodes/2];
    for (int i = 0; i < this->config->num_nodes/2; i++) {
        *(node_t*)ptr = i * 2;
        msgs[i] = Message(std::string(buf, BUFSIZE));
    }

    gettimeofday(&prev, nullptr);
    now = prev;
    while (true) {
        while (latency(prev, now) < this->interval) {
            gettimeofday(&now, nullptr);
        }
        prev = now;
        for (int i = 0; i < this->config->num_nodes/2; i++) {
            this->transport->send_message_to_router(msgs[i]);
        }
    }
}

} // namespace memcachekv
