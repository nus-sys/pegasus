#include <assert.h>
#include <sys/socket.h>
#include <cstring>

#include <transport.h>
#include <logger.h>

TransportReceiver::~TransportReceiver()
{
}

void TransportReceiver::register_transport(Transport *transport)
{
    this->transport = transport;
}

Transport::Transport(const Configuration *config)
    : config(config), receiver(nullptr)
{
};

Transport::~Transport() {}

void Transport::register_receiver(TransportReceiver *receiver)
{
    assert(receiver);
    this->receiver = receiver;
}

void Transport::send_message_to_node(const std::string &msg, int rack_id, int node_id)
{
    assert(rack_id < this->config->num_racks && rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message(msg,
                 *this->config->node_addresses.at(rack_id).at(node_id));
}

void Transport::send_message_to_local_node(const std::string &msg, int node_id)
{
    assert(this->config->rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message(msg,
                 *this->config->node_addresses.at(this->config->rack_id).at(node_id));
}

void Transport::send_message_to_router(const std::string &msg)
{
    send_message(msg, *this->config->router_address);
}

void Transport::send_message_to_controller(const std::string &msg, int rack_id)
{
    assert(rack_id >= 0 && rack_id < (int)this->config->controller_addresses.size());
    send_message(msg, *this->config->controller_addresses.at(rack_id));
}
