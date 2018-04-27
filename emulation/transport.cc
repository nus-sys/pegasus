#include <assert.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <event2/thread.h>
#include "transport.h"
#include "logger.h"

Transport::Transport(int dscp)
    : dscp(dscp), event_base(nullptr), socket_fd(-1), controller_fd(-1)
{
    evthread_use_pthreads();
};

Transport::~Transport()
{
    if (this->socket_fd > 0) {
        close(socket_fd);
    }

    if (this->controller_fd > 0) {
        close(controller_fd);
    }

    for (auto event : this->events) {
        event_free(event);
    }

    if (this->event_base != nullptr) {
        event_base_free(this->event_base);
    }
}

void
Transport::register_node(TransportReceiver *receiver,
                         Configuration *config,
                         int node_id)
{
    if (node_id < 0) {
        // Client node
        register_address(receiver, config, NodeAddress());
    } else {
        assert(node_id < config->num_nodes);
        register_address(receiver, config, config->addresses.at(node_id));
        listen_on_router(config->addresses.at(node_id));
        listen_on_controller();
    }
}

void
Transport::register_router(TransportReceiver *receiver,
                           Configuration *config)
{
    assert(config->router_address.address.size() > 0);
    register_address(receiver, config, config->router_address);
}

void
Transport::run()
{
    event_base_dispatch(this->event_base);
}

void
Transport::stop()
{
    event_base_loopbreak(this->event_base);
}

void
Transport::send_message(const std::string &msg, const sockaddr &addr)
{
    if (sendto(this->socket_fd, msg.c_str(), msg.size()+1, 0, &addr, sizeof(addr)) == -1) {
        printf("Failed to send message\n");
    }
}

void
Transport::send_message_to_addr(const std::string &msg, const NodeAddress &addr)
{
    send_message(msg, *(struct sockaddr *)&addr.sin);
}

void
Transport::send_message_to_node(const std::string &msg, int dst_node_id)
{
    assert(dst_node_id < this->config->num_nodes);
    send_message_to_addr(msg, this->config->addresses.at(dst_node_id));
}

void
Transport::send_message_to_controller(const std::string &msg)
{
    if (sendto(this->controller_fd,
               msg.c_str(),
               msg.size()+1,
               0,
               (struct sockaddr *)&this->config->controller_address.sin,
               sizeof(this->config->controller_address.sin)) == -1) {
        printf("Failed to send message to controller\n");
    }
}

void
Transport::register_address(TransportReceiver *receiver,
                            Configuration *config,
                            const NodeAddress &node_addr)
{
    assert(receiver != nullptr);
    assert(config != nullptr);
    this->receiver = receiver;
    this->config = config;

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

    // Set DSCP
    if (this->dscp > 0) {
        n = this->dscp << 2;
        if (setsockopt(this->socket_fd, IPPROTO_IP, IP_TOS, (char *)&n, sizeof(n)) < 0) {
            panic("Failed to set IP_TOS");
        }
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

    // Create event base
    this->event_base = event_base_new();
    if (this->event_base == nullptr) {
        panic("Failed to create new libevent event base");
    }

    // Add socket event
    struct event *sock_ev = event_new(this->event_base,
                                      this->socket_fd,
                                      EV_READ | EV_PERSIST,
                                      socket_callback,
                                      (void *)this);
    if (sock_ev == nullptr) {
        panic("Failed to create new event");
    }

    event_add(sock_ev, NULL);
    this->events.push_back(sock_ev);

    // Add signal events
    struct event *term_ev = evsignal_new(this->event_base,
                                         SIGTERM,
                                         &Transport::signal_callback,
                                         (void *)this);
    struct event *int_ev = evsignal_new(this->event_base,
                                        SIGINT,
                                        signal_callback,
                                        (void *)this);
    event_add(term_ev, NULL);
    this->events.push_back(term_ev);
    event_add(int_ev, NULL);
    this->events.push_back(int_ev);
}

void
Transport::listen_on_router(const NodeAddress &node_addr)
{
    assert(this->config != nullptr);
    if (this->config->router_address.address.size() == 0) {
        return;
    }
    // Setup socket
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        panic("Failed to create socket");
    }

    // Non-blocking mode
    if (fcntl(fd, F_SETFL, O_NONBLOCK, 1) == -1) {
        panic("Failed to set O_NONBLOCK");
    }

    // Reuse address
    int n = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_REUSEADDR");
    }

    // Increase buffer size
    n = this->SOCKET_BUF_SIZE;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_RCVBUF");
    }
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (char *)&n, sizeof(n)) < 0) {
        panic("Failed to set SO_SNDBUF");
    }

    // Bind to address
    struct sockaddr_in sin = this->config->router_address.sin;
    sin.sin_port = node_addr.sin.sin_port;

    if (bind(fd, (sockaddr *)&sin, sizeof(sin)) != 0) {
        panic("Failed to bind port");
    }

    // Add socket event
    struct event *sock_ev = event_new(this->event_base,
                                      fd,
                                      EV_READ | EV_PERSIST,
                                      socket_callback,
                                      (void *)this);
    if (sock_ev == nullptr) {
        panic("Failed to create new event");
    }

    event_add(sock_ev, NULL);
    this->events.push_back(sock_ev);
}

void
Transport::listen_on_controller()
{
    assert(this->config != nullptr);
    if (this->config->controller_address.address.size() == 0) {
        return;
    }
    // Setup socket
    this->controller_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (this->controller_fd == -1) {
        panic("Failed to create socket");
    }

    // Non-blocking mode
    if (fcntl(this->controller_fd, F_SETFL, O_NONBLOCK, 1) == -1) {
        panic("Failed to set O_NONBLOCK");
    }

    // Reuse address
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

    // Add socket event
    struct event *sock_ev = event_new(this->event_base,
                                      this->controller_fd,
                                      EV_READ | EV_PERSIST,
                                      socket_callback,
                                      (void *)this);
    if (sock_ev == nullptr) {
        panic("Failed to create new event");
    }

    event_add(sock_ev, NULL);
    this->events.push_back(sock_ev);
}
void
Transport::socket_callback(evutil_socket_t fd, short what, void *arg)
{
    if (what & EV_READ) {
        Transport *transport = (Transport *)arg;
        transport->on_readable(fd);
    }
}

void
Transport::signal_callback(evutil_socket_t fd, short what, void *arg)
{
    Transport *transport = (Transport *)arg;
    transport->stop();
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

    this->receiver->receive_message(std::string(buf, ret), src_addr);
}
