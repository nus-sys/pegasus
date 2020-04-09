#include <configuration.h>

Address::~Address() { }

Configuration::Configuration()
    : num_racks(0), num_nodes(0), rack_id(-1),
    node_id(-1), client_id(-1), n_transport_threads(0),
    transport_core(-1), app_core(-1), is_server(false),
    terminating(false), router_address(nullptr)
{
}

Configuration::~Configuration()
{
    for (auto &rack : this->node_addresses) {
        for (auto &addr : rack) {
            delete addr;
        }
    }
    for (auto &addr : this->client_addresses) {
        delete addr;
    }
    if (this->router_address != nullptr) {
        delete this->router_address;
    }
    for (auto &addr : this->controller_addresses) {
        delete addr;
    }
}

const Address *Configuration::my_address() const
{
    if (this->is_server) {
        return this->node_addresses.at(this->rack_id).at(this->node_id);
    } else {
        return this->client_addresses.at(this->client_id);
    }
}
