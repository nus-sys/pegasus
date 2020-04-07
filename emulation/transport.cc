#include <assert.h>
#include <sys/socket.h>
#include <cstring>

#include <transport.h>
#include <logger.h>

Message::Message()
    : buf_(nullptr), len_(0), dealloc_(false)
{
}

Message::Message(void *buf, size_t len, bool dealloc)
    : buf_(buf), len_(len), dealloc_(dealloc)
{
}

Message::Message(const std::string &str)
{
    this->buf_ = malloc(str.size());
    memcpy(this->buf_, str.data(), str.size());
    this->len_ = str.size();
    this->dealloc_ = true;
}

Message::~Message()
{
    if (this->dealloc_ && this->buf_ != nullptr) {
        free(this->buf_);
    }
}

const void *Message::buf() const
{
    return this->buf_;
}

size_t Message::len() const
{
    return this->len_;
}

void Message::set_message(void *buf, size_t len, bool dealloc)
{
    if (this->buf_ != nullptr) {
        free(this->buf_);
    }
    this->buf_ = buf;
    this->len_ = len;
    this->dealloc_ = dealloc;
}

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

void Transport::send_message_to_node(const Message &msg, int rack_id, int node_id)
{
    assert(rack_id < this->config->num_racks && rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message(msg,
                 *this->config->node_addresses.at(rack_id).at(node_id));
}

void Transport::send_message_to_local_node(const Message &msg, int node_id)
{
    assert(this->config->rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message(msg,
                 *this->config->node_addresses.at(this->config->rack_id).at(node_id));
}

void Transport::send_message_to_router(const Message &msg)
{
    send_message(msg, *this->config->router_address);
}

void Transport::send_message_to_controller(const Message &msg, int rack_id)
{
    assert(rack_id >= 0 && rack_id < (int)this->config->controller_addresses.size());
    send_message(msg, *this->config->controller_addresses.at(rack_id));
}
