#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_ethdev.h>

#include <logger.h>
#include <transports/dpdk/transport.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
static char *argv[] = {
    "command",
    "-l",
    "0-1",
};
#pragma GCC diagnostic pop

#define RTE_RX_DESC 1024
#define RTE_TX_DESC 1024
#define MAX_PKT_BURST 32
#define MEMPOOL_CACHE_SIZE 256

static int transport_thread(void *arg)
{
    uint16_t nb_rx, i, j;
    struct rte_mbuf *pkt_burst[MAX_PKT_BURST];
    struct rte_mbuf *m;
    DPDKTransport *transport = (DPDKTransport*)arg;

    while (transport->status == DPDKTransport::RUNNING) {
        nb_rx = rte_eth_rx_burst(transport->portid, 0, pkt_burst, MAX_PKT_BURST);
        for (i = 0; i < nb_rx; i++) {
            m = pkt_burst[i];
            struct rte_ether_hdr *eth_hdr;
            eth_hdr = rte_pktmbuf_mtod(m, struct rte_ether_hdr*);
            printf("SRC: ");
            for (j = 0; j < RTE_ETHER_ADDR_LEN; j++) {
                printf("%x ", eth_hdr->s_addr.addr_bytes[j]);
            }
            printf("DST: ");
            for (j = 0; j < RTE_ETHER_ADDR_LEN; j++) {
                printf("%x ", eth_hdr->d_addr.addr_bytes[j]);
            }
            printf("\n");
        }
    }
    return 0;
}

DPDKTransport::DPDKTransport(const Configuration *config)
    : Transport(config), portid(0), status(STOPPED)
{
    int argc = sizeof(argv) / sizeof(const char*);
    unsigned nb_mbufs;
    uint16_t nb_ports, nb_rxd = RTE_RX_DESC, nb_txd = RTE_TX_DESC;
    struct rte_eth_rxconf rxconf;
    struct rte_eth_txconf txconf;
    struct rte_eth_conf port_conf;
    struct rte_eth_dev_info dev_info;

    // Initialize
    if (rte_eal_init(argc, argv) < 0) {
        panic("rte_eal_init failed");
    }

    if ((nb_ports = rte_eth_dev_count_avail()) == 0) {
        panic("No available Ethernet ports");
    }

    // Create mbuf pool
    nb_mbufs = nb_rxd + nb_txd + MAX_PKT_BURST + MEMPOOL_CACHE_SIZE;
    this->pktmbuf_pool = rte_pktmbuf_pool_create("pktmbuf_pool",
                                                 nb_mbufs,
                                                 MEMPOOL_CACHE_SIZE,
                                                 0,
                                                 RTE_MBUF_DEFAULT_BUF_SIZE,
                                                 rte_socket_id());
    if (this->pktmbuf_pool == nullptr) {
        panic("rte_pktmbuf_pool_create failed");
    }

    // Initialize port
    port_conf.rxmode.split_hdr_size = 0;
    port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;

    if (rte_eth_dev_info_get(this->portid, &dev_info) != 0) {
        panic("rte_eth_dev_info_get failed");
    }
    if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE) {
        port_conf.txmode.offloads |= DEV_TX_OFFLOAD_MBUF_FAST_FREE;
    }
    if (rte_eth_dev_configure(this->portid, 1, 1, &port_conf) < 0) {
        panic("rte_eth_dev_configure failed");
    }
    if (rte_eth_dev_adjust_nb_rx_tx_desc(this->portid, &nb_rxd, &nb_txd) < 0) {
        panic("rte_eth_dev_adjust_nb_rx_tx_desc failed");
    }

    // Initialize RX queue
    rxconf = dev_info.default_rxconf;
    rxconf.offloads = port_conf.rxmode.offloads;
    if (rte_eth_rx_queue_setup(this->portid,
                               0,
                               nb_rxd,
                               rte_eth_dev_socket_id(this->portid),
                               &rxconf,
                               this->pktmbuf_pool) < 0) {
        panic("rte_eth_rx_queue_setup failed");
    }

    // Initialize TX queue
    txconf = dev_info.default_txconf;
    txconf.offloads = port_conf.txmode.offloads;
    if (rte_eth_tx_queue_setup(this->portid,
                               0,
                               nb_txd,
                               rte_eth_dev_socket_id(this->portid),
                               &txconf) < 0) {
        panic("rte_eth_tx_queue_setup failed");
    }

    // Start device
    if (rte_eth_dev_start(this->portid) < 0) {
        panic("rte_eth_dev_start failed");
    }
    if (rte_eth_promiscuous_enable(this->portid) != 0) {
        panic("rte_eth_promiscuous_enable failed");
    }
}

DPDKTransport::~DPDKTransport()
{
    rte_mempool_free(this->pktmbuf_pool);
    rte_eth_dev_stop(this->portid);
    rte_eth_dev_close(this->portid);
}

void DPDKTransport::send_message(const std::string &msg, const Address &addr)
{
}

void DPDKTransport::run(void)
{
    this->status = RUNNING;
    if (rte_eal_mp_remote_launch(transport_thread, this, SKIP_MASTER) != 0) {
        panic("rte_eal_mp_remote_launch failed");
    }
}

void DPDKTransport::stop(void)
{
    this->status = STOPPED;
}

void DPDKTransport::wait(void)
{
    uint16_t lcore_id;
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            printf("rte_eal_wait_lcore failed on core %d\n", lcore_id);
        }
    }
}
