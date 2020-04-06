#ifndef _MEMCACHEKV_DECREMENTOR_H_
#define _MEMCACHEKV_DECREMENTOR_H_

#include <application.h>
#include <configuration.h>

namespace memcachekv {

class Decrementor : public Application {
public:
    Decrementor(Configuration *config,
                int interval, int n_dec);
    ~Decrementor();

    virtual void receive_message(const Message &msg,
                                 const Address &addr) override final;
    virtual void run(int duration) override final;

private:
    Configuration *config;
    int interval;
    int n_dec;

    typedef uint16_t identifier_t;
    typedef uint8_t op_type_t;
    typedef uint32_t keyhash_t;
    typedef uint8_t node_t;
    typedef uint16_t load_t;
    typedef uint32_t ver_t;

    static const size_t BUFSIZE = sizeof(identifier_t) + sizeof(op_type_t) + sizeof(keyhash_t) + sizeof(node_t) + sizeof(load_t) + sizeof(ver_t) + sizeof(node_t) + sizeof(load_t);

    static const identifier_t PEGASUS = 0x4750;
    static const op_type_t DEC = 0xF;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_DECREMENTOR_H_ */
