#include <cassert>
#include <net/ethernet.h>
#include <netinet/in.h>
#include <netinet/ip.h>

#include <rte_eal.h>
#include <rte_lcore.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_malloc.h>

#include <logger.h>
#include <application.h>
#include <transports/dpdk/transport.h>
#include <transports/dpdk/configuration.h>

#define RTE_RX_DESC 4096
#define RTE_TX_DESC 4096
#define MAX_PKT_BURST 32
#define MEMPOOL_CACHE_SIZE 256

#define IPV4_HDR_SIZE 5
#define IPV4_TTL 0xFF

#define FLOW_DEFAULT_PRIORITY 1
#define FLOW_TRANSPORT_PRIORITY 0
#define FLOW_PATTERN_NUM 4
#define FLOW_ACTION_NUM 2
#define FLOW_IPV4_ADDR_MASK 0xFFFFFFFF
#define FLOW_UDP_PORT_MASK 0xFFFF

#define ETH_RSS_HF ETH_RSS_NONFRAG_IPV4_UDP

#define DEFAULT_PORT_ID 0

thread_local static int rx_queue_id;
thread_local static int tx_queue_id;
thread_local static uint16_t rand_port;
#define RAND_PORT_BASE 12345
#define RAND_PORT_MAX 10000

static bool use_tx_buffer;
static size_t tx_buffer_size;
thread_local static struct rte_eth_dev_tx_buffer *tx_buffer;

struct AppArg {
    Application *app;
    int tid;
    int tx_queue_id;
    int dev_port;
};

struct TransportArg {
    DPDKTransport *transport;
    int tid;
    int rx_queue_id;
    int tx_queue_id;
    int dev_port;
};

#define MAX_THREADS 128

static struct TransportArg transport_args[MAX_THREADS];
static struct AppArg app_args[MAX_THREADS];

static void tx_buffer_init(int dev_port)
{
    tx_buffer = (rte_eth_dev_tx_buffer*)rte_zmalloc_socket(nullptr, RTE_ETH_TX_BUFFER_SIZE(tx_buffer_size), 0, rte_eth_dev_socket_id(dev_port));
    if (tx_buffer == nullptr) {
        panic("Failed to allocate TX buffer");
    }
    if (rte_eth_tx_buffer_init(tx_buffer, tx_buffer_size) != 0) {
        panic("Failed to initialize TX buffer");
    }
}

static int transport_thread_(void *arg)
{
    struct TransportArg *targ = (struct TransportArg*)arg;
    rx_queue_id = targ->rx_queue_id;
    tx_queue_id = targ->tx_queue_id;
    rand_port = rand() % RAND_PORT_MAX;
    if (use_tx_buffer) {
        tx_buffer_init(targ->dev_port);
    }
    targ->transport->transport_thread(targ->tid);
    if (use_tx_buffer) {
        rte_free(tx_buffer);
    }
    return 0;
}

static int app_thread(void *arg)
{
    struct AppArg *app_arg = (struct AppArg*)arg;
    tx_queue_id = app_arg->tx_queue_id;
    rand_port = rand() % RAND_PORT_MAX;
    if (use_tx_buffer) {
        tx_buffer_init(app_arg->dev_port);
    }
    app_arg->app->run_thread(app_arg->tid);
    if (use_tx_buffer) {
        rte_free(tx_buffer);
    }
    return 0;
}

static void construct_arguments(const Configuration *config, int argc, char **argv)
{
    argv[0] = new char[strlen("command")+1];
    strcpy(argv[0], "command");
    argv[1] = new char[strlen("-l")+1];
    strcpy(argv[1], "-l");
    std::string cores;
    char app_cores[16], transport_cores[16];
    if (config->n_app_threads > 0) {
        sprintf(app_cores, "%d-%d", config->app_core, config->app_core+config->n_app_threads-1);
        cores.append(app_cores);
    }
    sprintf(transport_cores, "%d-%d", config->transport_core, config->transport_core+config->n_transport_threads-1);
    cores.append(",");
    cores.append(transport_cores);
    argv[2] = new char[cores.length()+1];
    strcpy(argv[2], cores.c_str());
    argv[3] = new char[strlen("--proc-type=auto")+1];
    strcpy(argv[3], "--proc-type=auto");
    const DPDKAddress *addr = static_cast<const DPDKAddress*>(config->my_address());
    int index = 4;
    for (const auto &blacklist : addr->blacklist) {
        argv[index] = new char[strlen("-b")+1];
        strcpy(argv[index++], "-b");
        argv[index] = new char[blacklist.length()+1];
        strcpy(argv[index++], blacklist.c_str());
    }
}

static void generate_flow_rules(const Configuration *config, uint16_t dev_port)
{

    {
        /* Default flow rule: drop */
        struct rte_flow_attr attr;
        memset(&attr, 0, sizeof(struct rte_flow_attr));
        attr.priority = FLOW_DEFAULT_PRIORITY;
        attr.ingress = 1;
        struct rte_flow_item patterns[2];
        memset(patterns, 0, sizeof(patterns));
        struct rte_flow_item_eth eth_spec;
        struct rte_flow_item_eth eth_mask;
        memset(&eth_spec, 0, sizeof(struct rte_flow_item_eth));
        memset(&eth_mask, 0, sizeof(struct rte_flow_item_eth));
        patterns[0].type = RTE_FLOW_ITEM_TYPE_ETH;
        patterns[0].spec = &eth_spec;
        patterns[0].mask = &eth_mask;
        patterns[1].type = RTE_FLOW_ITEM_TYPE_END;
        struct rte_flow_action actions[2];
        actions[0].type = RTE_FLOW_ACTION_TYPE_DROP;
        actions[1].type = RTE_FLOW_ACTION_TYPE_END;
        if (rte_flow_validate(dev_port, &attr, patterns, actions, nullptr) != 0) {
            panic("Default flow rule is not valid");
        }
        if (rte_flow_create(dev_port, &attr, patterns, actions, nullptr) == nullptr) {
            panic("rte_flow_create failed");
        }
    }

    /* Configure receive flow rule for each colocated node */
    for (int colocate_id = 0; colocate_id < config->n_colocate_nodes; colocate_id++) {
        /* Node address */
        const DPDKAddress *addr;
        switch (config->node_type) {
        case Configuration::NodeType::SERVER:
            addr = static_cast<const DPDKAddress*>(config->node_addresses.at(config->rack_id).at(config->node_id+colocate_id));
            break;
        case Configuration::NodeType::CLIENT:
            assert(colocate_id == 0);
            addr = static_cast<const DPDKAddress*>(config->client_addresses.at(config->client_id+colocate_id));
            break;
        case Configuration::NodeType::LB:
            assert(colocate_id == 0);
            addr = static_cast<const DPDKAddress*>(config->lb_address);
            break;
        default:
            panic("Unreachable");
        }
        // Each node gets one rx queue
        uint16_t rx_queue_id = colocate_id;

        /* Attributes */
        struct rte_flow_attr attr;
        memset(&attr, 0, sizeof(struct rte_flow_attr));
        attr.priority = FLOW_TRANSPORT_PRIORITY;
        attr.ingress = 1;

        /* Header match */
        // Ethernet
        struct rte_flow_item patterns[FLOW_PATTERN_NUM];
        memset(patterns, 0, sizeof(patterns));
        struct rte_flow_item_eth eth_spec;
        struct rte_flow_item_eth eth_mask;
        memset(&eth_spec, 0, sizeof(struct rte_flow_item_eth));
        memset(&eth_mask, 0, sizeof(struct rte_flow_item_eth));
        memcpy(&eth_spec.dst, &addr->ether_addr, sizeof(struct rte_ether_addr));
        memset(&eth_mask.dst, 0xFF, sizeof(struct rte_ether_addr));
        patterns[0].type = RTE_FLOW_ITEM_TYPE_ETH;
        patterns[0].spec = &eth_spec;
        patterns[0].mask = &eth_mask;
        // IPv4
        struct rte_flow_item_ipv4 ip_spec;
        struct rte_flow_item_ipv4 ip_mask;
        memset(&ip_spec, 0, sizeof(struct rte_flow_item_ipv4));
        memset(&ip_mask, 0, sizeof(struct rte_flow_item_ipv4));
        ip_spec.hdr.dst_addr = addr->ip_addr;
        ip_mask.hdr.dst_addr = FLOW_IPV4_ADDR_MASK;
        patterns[1].type = RTE_FLOW_ITEM_TYPE_IPV4;
        patterns[1].spec = &ip_spec;
        patterns[1].mask = &ip_mask;
        // UDP
        struct rte_flow_item_udp udp_spec;
        struct rte_flow_item_udp udp_mask;
        memset(&udp_spec, 0, sizeof(struct rte_flow_item_udp));
        memset(&udp_mask, 0, sizeof(struct rte_flow_item_udp));
        udp_spec.hdr.dst_port = addr->udp_port;
        udp_mask.hdr.dst_port = FLOW_UDP_PORT_MASK;
        patterns[2].type = RTE_FLOW_ITEM_TYPE_UDP;
        patterns[2].spec = &udp_spec;
        patterns[2].mask = &udp_mask;

        patterns[3].type = RTE_FLOW_ITEM_TYPE_END;

        /* Actions: forward to queues */
        struct rte_flow_action actions[FLOW_ACTION_NUM];
        struct rte_flow_action_queue action_queue;
        memset(actions, 0, sizeof(actions));
        action_queue.index = rx_queue_id;
        actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
        actions[0].conf = &action_queue;
        actions[1].type = RTE_FLOW_ACTION_TYPE_END;

        /* Validate and install flow rules */
        if (rte_flow_validate(dev_port, &attr, patterns, actions, nullptr) != 0) {
            panic("Flow rule is not valid");
        }
        if (rte_flow_create(dev_port, &attr, patterns, actions, nullptr) == nullptr) {
            panic("rte_flow_create failed");
        }
    }
}

DPDKTransport::DPDKTransport(const Configuration *config, bool use_flow_api)
    : Transport(config), use_flow_api(use_flow_api), status(STOPPED)
{
    uint16_t nb_ports, nb_rxd = RTE_RX_DESC, nb_txd = RTE_TX_DESC;
    struct rte_eth_rxconf rxconf;
    struct rte_eth_txconf txconf;
    struct rte_eth_conf port_conf;
    struct rte_eth_dev_info dev_info;

    // Initialize
    const DPDKAddress *addr = static_cast<const DPDKAddress*>(config->my_address());
    this->dev_port = addr->dev_port;
    use_tx_buffer = static_cast<const DPDKConfiguration*>(config)->use_tx_buffer;
    tx_buffer_size = static_cast<const DPDKConfiguration*>(config)->tx_buffer_size;

    this->argc = 4 + (addr->blacklist.size() * 2);
    this->argv = new char*[this->argc];
    construct_arguments(config, this->argc, this->argv);
    if (rte_eal_init(argc, argv) < 0) {
        panic("rte_eal_init failed");
    }

    if ((nb_ports = rte_eth_dev_count_avail()) == 0) {
        panic("No available Ethernet ports");
    }

    // Initialize mempool
    unsigned nb_mbufs = (config->n_app_threads + config->n_transport_threads) * (nb_rxd + nb_txd + MAX_PKT_BURST + MEMPOOL_CACHE_SIZE);
    char pool_name[32];
    sprintf(pool_name, "pktmbuf_pool");
    this->pktmbuf_pool = rte_pktmbuf_pool_create(pool_name,
                                                 nb_mbufs,
                                                 MEMPOOL_CACHE_SIZE,
                                                 0,
                                                 RTE_MBUF_DEFAULT_BUF_SIZE,
                                                 rte_socket_id());
    if (this->pktmbuf_pool == nullptr) {
        panic("rte_pktmbuf_pool_create failed");
    }

    // Initialize port
    memset(&port_conf, 0, sizeof(port_conf));
    port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;
    port_conf.rx_adv_conf.rss_conf.rss_key = nullptr;
    port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_HF;

    if (rte_eth_dev_info_get(this->dev_port, &dev_info) != 0) {
        panic("rte_eth_dev_info_get failed");
    }
    if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE) {
        port_conf.txmode.offloads |= DEV_TX_OFFLOAD_MBUF_FAST_FREE;
    }
    int num_rx_queues = config->n_transport_threads;
    int num_tx_queues = config->n_app_threads + config->n_transport_threads;
    if (rte_eth_dev_configure(this->dev_port,
                              num_rx_queues,
                              num_tx_queues,
                              &port_conf) < 0) {
        panic("rte_eth_dev_configure failed");
    }
    if (rte_eth_dev_adjust_nb_rx_tx_desc(this->dev_port, &nb_rxd, &nb_txd) < 0) {
        panic("rte_eth_dev_adjust_nb_rx_tx_desc failed");
    }

    // Initialize RX queue
    rxconf = dev_info.default_rxconf;
    rxconf.offloads = port_conf.rxmode.offloads;
    for (int qid = 0; qid < num_rx_queues; qid++) {
        if (rte_eth_rx_queue_setup(this->dev_port,
                                   qid,
                                   nb_rxd,
                                   rte_eth_dev_socket_id(this->dev_port),
                                   &rxconf,
                                   this->pktmbuf_pool) < 0) {
            panic("rte_eth_rx_queue_setup failed");
        }
    }

    // Initialize TX queue
    txconf = dev_info.default_txconf;
    txconf.offloads = port_conf.txmode.offloads;
    for (int qid = 0; qid < num_tx_queues; qid++) {
        if (rte_eth_tx_queue_setup(this->dev_port,
                                   qid,
                                   nb_txd,
                                   rte_eth_dev_socket_id(this->dev_port),
                                   &txconf) < 0) {
            panic("rte_eth_tx_queue_setup failed");
        }
    }

    // Start device
    if (rte_eth_dev_start(this->dev_port) < 0) {
        panic("rte_eth_dev_start failed");
    }
    if (rte_eth_promiscuous_enable(this->dev_port) != 0) {
        panic("rte_eth_promiscuous_enable failed");
    }

    // Create flow rules
    if (this->use_flow_api) {
        generate_flow_rules(config, this->dev_port);
    }
}

DPDKTransport::~DPDKTransport()
{
    if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
        rte_flow_flush(this->dev_port, nullptr);
        rte_eth_dev_stop(this->dev_port);
        rte_eth_dev_close(this->dev_port);
    }
    if (this->argv != nullptr) {
        for (int i = 0; i < this->argc; i++) {
            delete this->argv[i];
        }
        delete [] this->argv;
    }
}

void DPDKTransport::send_message(const Message &msg, const Address &addr)
{
    struct rte_mbuf *m;
    struct rte_ether_hdr *ether_hdr;
    struct rte_ipv4_hdr *ip_hdr;
    struct rte_udp_hdr *udp_hdr;
    void *dgram;
    const DPDKAddress &dst_addr = static_cast<const DPDKAddress&>(addr);
    const DPDKAddress &src_addr = static_cast<const DPDKAddress&>(*this->config->my_address());

    /* Allocate mbuf */
    m = rte_pktmbuf_alloc(this->pktmbuf_pool);
    if (m == nullptr) {
        panic("Failed to allocate rte_mbuf");
    }
    /* Ethernet header */
    ether_hdr = (struct rte_ether_hdr*)rte_pktmbuf_append(m, ETHER_HDR_LEN);
    if (ether_hdr == nullptr) {
        panic("Failed to allocate Ethernet header");
    }
    ether_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
    memcpy(&ether_hdr->d_addr, &dst_addr.ether_addr, sizeof(struct rte_ether_addr));
    memcpy(&ether_hdr->s_addr, &src_addr.ether_addr, sizeof(struct rte_ether_addr));
    /* IP header */
    ip_hdr = (struct rte_ipv4_hdr*)rte_pktmbuf_append(m, IPV4_HDR_SIZE * RTE_IPV4_IHL_MULTIPLIER);
    if (ip_hdr == nullptr) {
        panic("Failed to allocated IP header");
    }
    ip_hdr->version_ihl = (IPVERSION << 4) | IPV4_HDR_SIZE;
    ip_hdr->type_of_service = 0;
    ip_hdr->total_length = rte_cpu_to_be_16(IPV4_HDR_SIZE * RTE_IPV4_IHL_MULTIPLIER +
                                            sizeof(struct rte_udp_hdr) +
                                            msg.len());
    ip_hdr->packet_id = 0;
    ip_hdr->fragment_offset = 0;
    ip_hdr->time_to_live = IPV4_TTL;
    ip_hdr->next_proto_id = IPPROTO_UDP;
    ip_hdr->hdr_checksum = 0;
    ip_hdr->src_addr = src_addr.ip_addr;
    ip_hdr->dst_addr = dst_addr.ip_addr;
    ip_hdr->hdr_checksum = rte_ipv4_cksum(ip_hdr);
    /* UDP header */
    udp_hdr = (struct rte_udp_hdr*)rte_pktmbuf_append(m, sizeof(struct rte_udp_hdr));
    if (udp_hdr == nullptr) {
        panic("Failed to allocate UDP header");
    }
    // Use random src port for RSS
    udp_hdr->src_port = RAND_PORT_BASE + (rand_port++ % RAND_PORT_MAX);
    udp_hdr->dst_port = dst_addr.udp_port;
    udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + msg.len());
    udp_hdr->dgram_cksum = 0;
    /* Datagram */
    dgram = rte_pktmbuf_append(m, msg.len());
    if (dgram == nullptr) {
        panic("Failed to allocate data gram");
    }
    memcpy(dgram, msg.buf(), msg.len());
    /* Send packet */
    if (use_tx_buffer) {
        rte_eth_tx_buffer(this->dev_port, tx_queue_id, tx_buffer, m);
    } else {
        if (rte_eth_tx_burst(this->dev_port, tx_queue_id, &m, 1) < 1) {
            rte_pktmbuf_free(m);
        }
    }
}

void DPDKTransport::send_raw(const void *buf, void *tdata)
{
    struct rte_mbuf *m;

    m = (struct rte_mbuf*)tdata;
    if (use_tx_buffer) {
        rte_eth_tx_buffer(this->dev_port, tx_queue_id, tx_buffer, m);
    } else {
        if (rte_eth_tx_burst(this->dev_port, tx_queue_id, &m, 1) < 1) {
            rte_pktmbuf_free(m);
        }
    }
}

void DPDKTransport::run(void)
{
    this->status = RUNNING;
    // Start all transport threads
    for (int tid = 1; tid < this->config->n_transport_threads; tid++) {
        transport_args[tid].transport = this;
        transport_args[tid].tid = this->config->n_app_threads + tid;
        transport_args[tid].rx_queue_id = tid;
        transport_args[tid].tx_queue_id = tid;
        transport_args[tid].dev_port = this->dev_port;
        if (rte_eal_remote_launch(transport_thread_,
                                  &transport_args[tid],
                                  this->config->transport_core + tid) != 0) {
            panic("transport thread rte_eal_remote_launch failed");
        }
    }

    transport_args[0].transport = this;
    transport_args[0].tid = this->config->n_app_threads;
    transport_args[0].rx_queue_id = 0;
    transport_args[0].tx_queue_id = 0;
    transport_args[0].dev_port = this->dev_port;

    if (this->config->n_app_threads > 0) {
        if (rte_eal_remote_launch(transport_thread_,
                                  &transport_args[0],
                                  this->config->transport_core) != 0) {
            panic("transport thread rte_eal_remote_launch failed");
        }
    } else {
        transport_thread_(&transport_args[0]);
    }
}

void DPDKTransport::stop(void)
{
    this->status = STOPPED;
}

void DPDKTransport::wait(void)
{
    for (int tid = 0; tid < this->config->n_transport_threads; tid++) {
        if (rte_eal_wait_lcore(this->config->transport_core + tid) < 0) {
            panic("rte_eal_wait_lcore failed on transport core");
        }
    }
}

void DPDKTransport::run_app_threads(Application *app)
{
    assert(this->config->n_app_threads <= this->config->n_transport_threads);
    // Launch app on slave cores first
    for (int tid = 1; tid < this->config->n_app_threads; tid++) {
        app_args[tid].app = app;
        app_args[tid].tid = tid;
        app_args[tid].tx_queue_id = this->config->n_transport_threads + tid;
        app_args[tid].dev_port = this->dev_port;
        if (rte_eal_remote_launch(app_thread,
                                  &app_args[tid],
                                  this->config->app_core + tid) != 0) {
            panic("app thread rte_eal_remote_launch failed");
        }
    }

    // Run on master core
    app_args[0].app = app;
    app_args[0].tid = 0;
    app_args[0].tx_queue_id = this->config->n_transport_threads;
    app_args[0].dev_port = this->dev_port;
    app_thread(&app_args[0]);

    // Wait for app slave cores to finish
    for (int tid = 1; tid < this->config->n_app_threads; tid++) {
        if (rte_eal_wait_lcore(this->config->app_core + tid) < 0) {
            panic("rte_eal_wait_lcore failed on app core");
        }
    }
}

void DPDKTransport::transport_thread(int tid)
{
    uint16_t n_rx, i;
    struct rte_mbuf *pkt_burst[MAX_PKT_BURST];
    struct rte_mbuf *m;
    size_t offset;

    while (this->status == DPDKTransport::RUNNING) {
        n_rx = rte_eth_rx_burst(this->dev_port,
                                rx_queue_id,
                                pkt_burst,
                                MAX_PKT_BURST);
        for (i = 0; i < n_rx; i++) {
            m = pkt_burst[i];
            if (this->config->use_raw_transport) {
                if (!this->receiver->receive_raw(rte_pktmbuf_mtod_offset(m, void*, 0),
                                                 m,
                                                 tid)) {
                    rte_pktmbuf_free(m);
                }
            } else {
                /* Parse packet header */
                struct rte_ether_hdr *ether_hdr;
                struct rte_ipv4_hdr *ip_hdr;
                struct rte_udp_hdr *udp_hdr;
                offset = 0;
                ether_hdr = rte_pktmbuf_mtod_offset(m, struct rte_ether_hdr*, offset);
                offset += ETHER_HDR_LEN;
                ip_hdr = rte_pktmbuf_mtod_offset(m, struct rte_ipv4_hdr*, offset);
                offset += (ip_hdr->version_ihl & RTE_IPV4_HDR_IHL_MASK) * RTE_IPV4_IHL_MULTIPLIER;
                udp_hdr = rte_pktmbuf_mtod_offset(m, struct rte_udp_hdr*, offset);
                offset += sizeof(struct rte_udp_hdr);

                if (this->use_flow_api || filter_packet(DPDKAddress(ether_hdr->d_addr,
                                                                    ip_hdr->dst_addr,
                                                                    udp_hdr->dst_port,
                                                                    DEFAULT_PORT_ID))) {
                    /* Construct source address */
                    DPDKAddress addr(ether_hdr->s_addr,
                                     ip_hdr->src_addr,
                                     udp_hdr->src_port,
                                     DEFAULT_PORT_ID);
                    /* Upcall to transport receiver */
                    Message msg(rte_pktmbuf_mtod_offset(m, void*, offset),
                            rte_be_to_cpu_16(udp_hdr->dgram_len)-sizeof(struct rte_udp_hdr),
                            false);
                    this->receiver->receive_message(msg, addr, tid);
                }
                rte_pktmbuf_free(m);
            }
        }
    }
}

bool DPDKTransport::filter_packet(const DPDKAddress &addr) const
{
    const DPDKAddress *my_addr = static_cast<const DPDKAddress*>(this->config->my_address());

    if (memcmp(&addr.ether_addr, &my_addr->ether_addr, sizeof(struct rte_ether_addr)) != 0) {
        return false;
    }
    if (addr.ip_addr != my_addr->ip_addr) {
        return false;
    }
    return true;
}
