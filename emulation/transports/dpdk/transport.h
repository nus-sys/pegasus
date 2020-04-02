#ifndef _DPDK_TRANSPORT_H_
#define _DPDK_TRANSPORT_H_

#include <rte_mempool.h>

#include <transport.h>

class DPDKTransport : public Transport {
public:
    DPDKTransport(const Configuration *config);
    ~DPDKTransport();

    virtual void send_message(const std::string &msg, const Address &addr) override final;
    virtual void run(void) override final;
    virtual void stop(void) override final;
    virtual void wait(void) override final;

    uint16_t portid;
    volatile enum {
        RUNNING,
        STOPPED,
    } status;

private:
    struct rte_mempool *pktmbuf_pool;
};

#endif /* _DPDK_TRANSPORT_H_ */
