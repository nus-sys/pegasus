#include <configuration.h>
#include <logger.h>

Address::~Address() { }

Configuration::Configuration()
    : duration(0), num_racks(0), num_nodes(0), rack_id(-1), node_id(-1),
    client_id(-1), transport_core(-1), n_transport_threads(0), app_core(-1),
    n_app_threads(0), colocate_id(-1), n_colocate_nodes(0),
    node_type(Configuration::NodeType::CLIENT), terminating(false),
    use_raw_transport(false), lb_address(nullptr)
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
    if (this->lb_address != nullptr) {
        delete this->lb_address;
    }
    for (auto &addr : this->controller_addresses) {
        delete addr;
    }
}

const Address *Configuration::my_address() const
{
    switch (this->node_type) {
    case Configuration::NodeType::SERVER:
        return this->node_addresses.at(this->rack_id).at(this->node_id);
    case Configuration::NodeType::CLIENT:
        return this->client_addresses.at(this->client_id);
    case Configuration::NodeType::LB:
        return this->lb_address;
    default:
        panic("Unreachable");
    }
}
