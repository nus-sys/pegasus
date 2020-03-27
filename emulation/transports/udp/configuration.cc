#include <assert.h>
#include <netdb.h>
#include <fstream>
#include <string.h>

#include <logger.h>
#include <transports/udp/configuration.h>

UDPAddress::UDPAddress(const std::string &address, const std::string &port)
{
    struct addrinfo hints, *res;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_flags = AI_PASSIVE;
    if (getaddrinfo(address.c_str(),
                    port.c_str(),
                    &hints,
                    &res) != 0) {
        panic("Failed to get address info");
    }
    assert(res->ai_family == AF_INET);
    assert(res->ai_socktype == SOCK_DGRAM);
    assert(res->ai_addr->sa_family == AF_INET);
    this->saddr = *res->ai_addr;
    freeaddrinfo(res);
}

UDPAddress::UDPAddress(const struct sockaddr &saddr)
    : saddr(saddr)
{
}

void UDPConfiguration::load_from_file(const char *file_path)
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

            char *host = strtok(arg, ":");
            char *port = strtok(nullptr, "");

            if (host == nullptr || port == nullptr) {
                panic("Configuration line format: 'node host:port'");
            }
            rack.push_back(new UDPAddress(std::string(host), std::string(port)));
        } else if (strcasecmp(cmd, "router") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'router' configuration line requires an argument");
            }

            char *host = strtok(arg, ":");
            char *port = strtok(nullptr, "");

            if (host == nullptr || port == nullptr) {
                panic("Configuration line format: 'router host:port'");
            }
            this->router_address = new UDPAddress(std::string(host), std::string(port));
        } else if (strcasecmp(cmd, "controller") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'controller' configuration line requires an argument");
            }

            char *host = strtok(arg, ":");
            char *port = strtok(nullptr, "");

            if (host == nullptr || port == nullptr) {
                panic("Configuration line format: 'controller host:port'");
            }
            this->controller_addresses.push_back(new UDPAddress(std::string(host), std::string(port)));
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
