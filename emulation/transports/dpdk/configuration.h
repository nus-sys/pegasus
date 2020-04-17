#ifndef _DPDK_CONFIGURATION_H_
#define _DPDK_CONFIGURATION_H_

#include <rte_ether.h>
#include <rte_byteorder.h>

#include <configuration.h>

class DPDKAddress : public Address {
public:
    DPDKAddress(const char *ether, const char *ip, const char *port);
    DPDKAddress(const struct rte_ether_addr &ether_addr,
                rte_be32_t ip_addr,
                rte_be16_t udp_port);

    struct rte_ether_addr ether_addr;
    rte_be32_t ip_addr;
    rte_be16_t udp_port;
};

class DPDKConfiguration : public Configuration {
public:
    DPDKConfiguration(const char *file_path);
};

#endif /* _DPDK_CONFIGURATION_H_ */
