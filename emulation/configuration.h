#ifndef _CONFIGURATION_H_
#define _CONFIGURATION_H_

#include <vector>
#include <string>

class Address {
public:
    virtual ~Address() = 0;
};

class Configuration {
public:
    Configuration();
    virtual ~Configuration() = 0;

    virtual void load_from_file(const char *file_path) = 0;

    int num_racks;
    int num_nodes;
    int rack_id;
    int node_id;
    int client_id;
    int n_transport_threads;
    int transport_core;
    int app_core;
    bool is_server;
    bool terminating;
    std::vector<std::vector<Address*>> node_addresses;
    std::vector<Address*> client_addresses;
    Address *router_address;
    std::vector<Address*> controller_addresses;
};

#endif /* _CONFIGURATION_H_ */
