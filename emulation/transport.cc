#include <assert.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <event2/thread.h>
#include "transport.h"
#include "logger.h"

Transport::Transport(const Configuration *config)
    : config(config), receiver(nullptr), socket_fd(-1), controller_fd(-1)
{
    if (config->node_id < 0) {
        // Client node
        register_address(NodeAddress());
    } else {
        // Server node
        assert(config->rack_id < config->num_racks);
        assert(config->node_id < config->num_nodes);
        register_address(config->addresses.at(config->rack_id).at(config->node_id));
    }
    register_controller();
};

Transport::~Transport()
{
    if (this->socket_fd > 0) {
        close(this->socket_fd);
    }
    if (this->controller_fd > 0) {
        close(this->controller_fd);
    }
}

void
Transport::register_receiver(TransportReceiver *receiver)
{
    assert(receiver);
    this->receiver = receiver;
}

void
Transport::send_message(const std::string &msg, const sockaddr &addr)
{
    if (sendto(this->socket_fd, msg.c_str(), msg.size(), 0, &addr, sizeof(addr)) == -1) {
        printf("Failed to send message\n");
    }
}

void
Transport::send_message_to_addr(const std::string &msg, const NodeAddress &addr)
{
    send_message(msg, *(struct sockaddr *)&addr.sin);
}

void
Transport::send_message_to_node(const std::string &msg, int rack_id, int node_id)
{
    assert(rack_id < this->config->num_racks && rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message_to_addr(msg, this->config->addresses.at(rack_id).at(node_id));
}

void
Transport::send_message_to_local_node(const std::string &msg, int node_id)
{
    assert(this->config->rack_id >= 0);
    assert(node_id < this->config->num_nodes && node_id >= 0);
    send_message_to_addr(msg, this->config->addresses.at(this->config->rack_id).at(node_id));
}

void
Transport::send_message_to_router(const std::string &msg)
{
    send_message_to_addr(msg, this->config->router_address);
}

void
Transport::send_message_to_controller(const std::string &msg)
{
    // Controller is on a different subnet
    if (sendto(this->controller_fd, msg.c_str(), msg.size(), 0,
               (struct sockaddr *)&this->config->controller_address.sin,
               sizeof(this->config->controller_address.sin)) == -1) {
        printf("Failed to send message to controller\n");
    }
}

void
Transport::register_address(const NodeAddress &node_addr)
{
    // Setup socket
    this->socket_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (this->socket_fd == -1) {
        panic("Failed to create socket");
    }

    // Non-blocking mode
    if (fcntl(this->socket_fd, F_SETFL, O_NONBLOCK, 1) == -1) {
        panic("Failed to set O_NONBLOCK");
    }

    // Enable outgoing broadcast
    int n = 1;
    if (setsockopt(this->socket_fd, SOL_SOCKET, SO_BROADCAST, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_BROADCAST");
    }

    // Disable UDP checksum
    n = 1;
    if (setsockopt(this->socket_fd, SOL_SOCKET, SO_NO_CHECK, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_NO_CHECK");
    }

    // Increase buffer size
    n = this->SOCKET_BUF_SIZE;
    if (setsockopt(this->socket_fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_RCVBUF");
    }
    if (setsockopt(this->socket_fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_SNDBUF");
    }

    // Bind to address
    struct sockaddr_in sin;
    if (node_addr.address.size() == 0) {
        // Client can bind to any port
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = 0;
    } else {
        sin = node_addr.sin;
    }

    if (bind(this->socket_fd, (sockaddr *)&sin, sizeof(sin)) != 0) {
        panic("Failed to bind port");
    }
}

void
Transport::register_controller()
{
    // Setup socket
    this->controller_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (this->controller_fd == -1) {
        panic("Failed to create socket");
    }

    // Non-blocking mode
    if (fcntl(this->controller_fd, F_SETFL, O_NONBLOCK, 1) == -1) {
        panic("Failed to set O_NONBLOCK");
    }

    int n = 1;
    if (setsockopt(this->controller_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_REUSEADDR");
    }

    // Bind to any address
    struct sockaddr_in sin;
    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = 0;

    if (bind(this->controller_fd, (sockaddr *)&sin, sizeof(sin)) != 0) {
        panic("Failed to bind port");
    }
}

void
Transport::on_readable(int fd)
{
    const int BUFSIZE = 65535;
    char buf[BUFSIZE];
    struct sockaddr src_addr;
    socklen_t addrlen = sizeof(src_addr);
    ssize_t ret;

    ret = recvfrom(fd, buf, BUFSIZE, 0, &src_addr, &addrlen);
    if (ret == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        printf("Failed to receive message\n");
    }

    assert(this->receiver);
    this->receiver->receive_message(std::string(buf, ret), src_addr);
}

TransportEventBase::TransportEventBase(Transport *transport)
    : transport(transport)
{
    evthread_use_pthreads();

    // Create event base
    this->event_base = event_base_new();
    if (this->event_base == nullptr) {
        panic("Failed to create new libevent event base");
    }

    // Add socket events
    if (transport->socket_fd > 0) {
        add_socket_event(transport->socket_fd);
    }
    if (transport->controller_fd > 0) {
        add_socket_event(transport->controller_fd);
    }
}

TransportEventBase::~TransportEventBase()
{
    for (auto event : this->events) {
        event_free(event);
    }

    if (this->event_base != nullptr) {
        event_base_free(this->event_base);
    }
}

void
TransportEventBase::run()
{
    event_base_dispatch(this->event_base);
}

void
TransportEventBase::stop()
{
    event_base_loopbreak(this->event_base);
}

void
TransportEventBase::add_socket_event(int fd)
{
    struct event *sock_ev = event_new(this->event_base,
                                      fd,
                                      EV_READ | EV_PERSIST,
                                      socket_callback,
                                      (void *)this->transport);
    if (sock_ev == nullptr) {
        panic("Failed to create new event");
    }

    event_add(sock_ev, nullptr);
    this->events.push_back(sock_ev);
}

void
TransportEventBase::socket_callback(evutil_socket_t fd, short what, void *arg)
{
    if (what & EV_READ) {
        Transport *transport = (Transport *)arg;
        transport->on_readable(fd);
    }
}

void
TransportEventBase::signal_callback(evutil_socket_t fd, short what, void *arg)
{
    TransportEventBase *eb = (TransportEventBase *)arg;
    eb->stop();
}
