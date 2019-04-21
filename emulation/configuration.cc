#include <assert.h>
#include <netdb.h>
#include <fstream>
#include <string.h>
#include "logger.h"
#include "configuration.h"

using std::string;

NodeAddress::NodeAddress()
    : address(""), port("")
{
}

NodeAddress::NodeAddress(const string &address, const string &port)
    : address(address), port(port)
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
    this->sin = *(sockaddr_in *)res->ai_addr;
    freeaddrinfo(res);
}

Configuration::Configuration(const std::vector<std::vector<NodeAddress>> &addresses,
                             const NodeAddress &router_address,
                             const NodeAddress &controller_address,
                             int rack_id,
                             int node_id,
                             int n_transport_threads,
                             int core_id,
                             bool terminating)
    : num_racks(addresses.size()), rack_id(rack_id), node_id(node_id),
    n_transport_threads(n_transport_threads), core_id(core_id),
    terminating(terminating), addresses(addresses), router_address(router_address),
    controller_address(controller_address)
{
    this->num_nodes = addresses.empty() ? 0 : addresses[0].size();
}

Configuration::Configuration(const char *file_path)
    : rack_id(-1), node_id(-1),
    n_transport_threads(1), terminating(false)
{
    std::ifstream file;
    std::vector<NodeAddress> rack;
    file.open(file_path);
    if (!file) {
        panic("Failed to open configuration file");
    }

    while (!file.eof()) {
        string line;
        getline(file, line);

        // Ignore comments
        if ((line.size() == 0) || (line[0] == '#')) {
            continue;
        }

        char *cmd = strtok(&line[0], " \t");

        if (strcasecmp(cmd, "rack") == 0) {
            if (!rack.empty()) {
                this->addresses.push_back(rack);
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
            rack.push_back(NodeAddress(string(host), string(port)));
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
            this->router_address = NodeAddress(string(host), string(port));
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
            this->controller_address = NodeAddress(string(host), string(port));
        } else {
            panic("Unknown configuration directive");
        }
    }
    // last rack
    if (!rack.empty()) {
        this->addresses.push_back(rack);
    }
    file.close();
    this->num_racks = this->addresses.size();
    this->num_nodes = this->num_racks == 0 ? 0 : this->addresses[0].size();
    assert(this->num_racks > 0 && this->num_nodes > 0);
}
