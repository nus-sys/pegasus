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
    Configuration(const std::vector<std::vector<NodeAddress>> &addresses,
                  const NodeAddress &router_address,
                  const NodeAddress &controller_address,
                  int rack_id,
                  int node_id,
                  int n_transport_threads,
                  bool terminating);
    Configuration(const char *file_path);
    virtual ~Configuration() {};

    virtual int key_to_node_id(const std::string &key) = 0;

    int num_racks;
    int num_nodes;
    int rack_id;
    int node_id;
    int n_transport_threads;
    bool terminating;
    std::vector<std::vector<NodeAddress>> addresses;
    NodeAddress router_address;
    NodeAddress controller_address;
};

#endif /* __CONFIGURATION_H__ */
