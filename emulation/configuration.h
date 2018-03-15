#ifndef __CONFIGURATION_H__
#define __CONFIGURATION_H__

#include <map>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>

class NodeAddress {
public:
    NodeAddress();
    NodeAddress(const std::string &address, const std::string &port);
    ~NodeAddress() {};

    struct sockaddr_in sin;
    std::string address;
    std::string port;
};

class Configuration {
public:
    Configuration(int num_nodes,
                  const std::map<int, NodeAddress> &addresses,
                  const NodeAddress &router_address)
        : num_nodes(num_nodes),
        addresses(addresses),
        router_address(router_address) {};
    ~Configuration() {};

    int num_nodes;
    std::map<int, NodeAddress> addresses;
    NodeAddress router_address;
};

#endif /* __CONFIGURATION_H__ */
