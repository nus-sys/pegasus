#ifndef _DPDK_TRANSPORT_H_
#define _DPDK_TRANSPORT_H_

#include <rte_mempool.h>

#include <transport.h>
#include <transports/dpdk/configuration.h>

class DPDKTransport : public Transport {
public:
    DPDKTransport(const Configuration *config, bool use_flow_api);
    ~DPDKTransport();

    virtual void send_message(const Message &msg, const Address &addr) override final;
    virtual void send_raw(const void *buf, void *tdata) override final;
    virtual void run() override final;
    virtual void stop() override final;
    virtual void wait() override final;
    virtual void run_app_threads(Application *app) override final;

    void distributor_thread();
    void transport_thread(int tid);

private:
    bool filter_packet(const DPDKAddress &addr) const;

    bool use_flow_api;
    int argc;
    char **argv;
    uint16_t dev_port;
    volatile enum {
        RUNNING,
        STOPPED,
    } status;
    struct rte_mempool *pktmbuf_pool;
};

#endif /* _DPDK_TRANSPORT_H_ */
