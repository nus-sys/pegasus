#ifndef __CONFIGURATION_H__
#define __CONFIGURATION_H__

#include <vector>
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
    Configuration(const std::vector<NodeAddress> &addresses,
                  const NodeAddress &router_address);
    Configuration(const char *file_path);
    ~Configuration() {};

    int num_nodes;
    std::vector<NodeAddress> addresses;
    NodeAddress router_address;
};

#endif /* __CONFIGURATION_H__ */
