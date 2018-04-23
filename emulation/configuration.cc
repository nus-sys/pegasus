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

Configuration::Configuration(const std::vector<NodeAddress> &addresses,
                             const NodeAddress &router_address)
    : num_nodes(addresses.size()), addresses(addresses), router_address(router_address)
{
}


Configuration::Configuration(const char *file_path)
{
    std::ifstream file;
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

        if (strcasecmp(cmd, "node") == 0) {
            char *arg = strtok(nullptr, " \t");
            if (arg == nullptr) {
                panic("'node' configuration line requires an argument");
            }

            char *host = strtok(arg, ":");
            char *port = strtok(nullptr, "");

            if (host == nullptr || port == nullptr) {
                panic("Configuration line format: 'node host:port'");
            }
            this->addresses.push_back(NodeAddress(string(host), string(port)));
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
    file.close();
    this->num_nodes = this->addresses.size();
    assert(this->num_nodes > 0);
}
