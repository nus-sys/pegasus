#include <arpa/inet.h>
#include <cassert>
#include <fstream>

#include <logger.h>
#include <transports/dpdk/configuration.h>

DPDKAddress::DPDKAddress(const char *ether,
                         const char *ip,
                         const char *port)
{
    if (rte_ether_unformat_addr(ether, &this->ether_addr) != 0) {
        panic("Failed to parse ethernet address");
    }
    if (inet_pton(AF_INET, ip, &this->ip_addr) != 1) {
        panic("Failed to parse IP address");
    }
    this->udp_port = rte_cpu_to_be_16(uint16_t(std::stoul(port)));
}

DPDKAddress::DPDKAddress(const struct rte_ether_addr &ether_addr,
                         rte_be32_t ip_addr,
                         rte_be16_t udp_port)
    : ip_addr(ip_addr), udp_port(udp_port)
{
    memcpy(&this->ether_addr, &ether_addr, sizeof(struct rte_ether_addr));
}

DPDKConfiguration::DPDKConfiguration(const char *file_path)
{
    std::ifstream file;
    std::vector<Address*> rack;
    file.open(file_path);
    if (!file) {
        panic("Failed to open configuration file");
    }

    while (!file.eof()) {
        std::string line;
        getline(file, line);

        // Ignore comments
        if ((line.size() == 0) || (line[0] == '#')) {
            continue;
        }

        char *cmd = strtok(&line[0], " \t");

        if (strcasecmp(cmd, "rack") == 0) {
            if (!rack.empty()) {
                this->node_addresses.push_back(rack);
                rack.clear();
            }
        } else if (strcasecmp(cmd, "node") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'node' configuration line requires an argument");
            }

            char *ether = strtok(arg, "|");
            char *ip = strtok(nullptr, "|");
            char *port = strtok(nullptr, "");

            if (ether == nullptr || ip == nullptr || port == nullptr) {
                panic("Configuration line format: 'node ether|ip|port'");
            }
            rack.push_back(new DPDKAddress(ether, ip, port));
        } else if (strcasecmp(cmd, "client") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'client' configuration line requires an argument");
            }

            char *ether = strtok(arg, "|");
            char *ip = strtok(nullptr, "|");
            char *port = strtok(nullptr, "");

            if (ether == nullptr || ip == nullptr || port == nullptr) {
                panic("Configuration line format: 'client ether|ip|port'");
            }
            this->client_addresses.push_back(new DPDKAddress(ether,
                                                             ip,
                                                             port));
        } else if (strcasecmp(cmd, "lb") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'lb' configuration line requires an argument");
            }

            char *ether = strtok(arg, "|");
            char *ip = strtok(nullptr, "|");
            char *port = strtok(nullptr, "");

            if (ether == nullptr || ip == nullptr || port == nullptr) {
                panic("Configuration line format: 'lb ether|ip|port'");
            }
            this->lb_address = new DPDKAddress(ether, ip, port);
        } else if (strcasecmp(cmd, "controller") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'controller' configuration line requires an argument");
            }

            char *ether = strtok(arg, "|");
            char *ip = strtok(nullptr, "|");
            char *port = strtok(nullptr, "");

            if (ether == nullptr || ip == nullptr || port == nullptr) {
                panic("Configuration line format: 'controller ether|ip|port'");
            }

            this->controller_addresses.push_back(new DPDKAddress(ether,
                                                                 ip,
                                                                 port));
        } else {
            panic("Unknown configuration directive");
        }
    }
    // last rack
    if (!rack.empty()) {
        this->node_addresses.push_back(rack);
    }
    file.close();
    this->num_racks = this->node_addresses.size();
    this->num_nodes = this->num_racks == 0 ? 0 : this->node_addresses[0].size();
    assert(this->num_racks > 0 && this->num_nodes > 0);
    assert((int)this->controller_addresses.size() == this->num_racks);
}
