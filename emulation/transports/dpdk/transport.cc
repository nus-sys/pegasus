#include <rte_eal.h>
#include <rte_lcore.h>

#include <logger.h>
#include <transports/dpdk/transport.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
static char *argv[] = {
    "command",
    "-l",
    "0",
};
#pragma GCC diagnostic pop

static int lcore_hello(void *arg)
{
    unsigned int lcore_id;
    lcore_id = rte_lcore_id();
    printf("hello from core %u\n", lcore_id);
    return 0;
}

DPDKTransport::DPDKTransport(const Configuration *config)
    : Transport(config)
{
    int ret, argc = sizeof(argv) / sizeof(const char*);
    unsigned int lcore_id;

    if ((ret = rte_eal_init(argc, argv)) < 0) {
        panic("rte_eal_init failed");
    }
    rte_eal_mp_remote_launch(lcore_hello, NULL, CALL_MASTER);
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            printf("rte_eal_wait_lcore failed on core %d\n", lcore_id);
        }
    }
}

DPDKTransport::~DPDKTransport()
{
}

void DPDKTransport::send_message(const std::string &msg, const Address &addr)
{
}

void DPDKTransport::run(void)
{
}

void DPDKTransport::stop(void)
{
}

void DPDKTransport::wait(void)
{
}
