#ifndef _DPDK_TRANSPORT_H_
#define _DPDK_TRANSPORT_H_

#include <rte_mempool.h>

#include <transport.h>
#include <transports/dpdk/configuration.h>

class DPDKTransport : public Transport {
public:
    DPDKTransport(const Configuration *config);
    ~DPDKTransport();

    virtual void send_message(const Message &msg, const Address &addr) override final;
    virtual void run() override final;
    virtual void stop() override final;
    virtual void wait() override final;
    virtual void run_app_threads(Application *app) override final;

    void run_internal();

private:
    bool filter_packet(const DPDKAddress &addr) const;

    int argc;
    char **argv;
    struct rte_mempool *pktmbuf_pool;
    uint16_t portid;
    uint16_t rx_queue_id;
    uint16_t tx_queue_id;
    volatile enum {
        RUNNING,
        STOPPED,
    } status;
};

#endif /* _DPDK_TRANSPORT_H_ */
