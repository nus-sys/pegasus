#include <cstring>
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <event2/thread.h>
#include <event2/event.h>

#include <application.h>
#include <transports/udp/configuration.h>
#include <transports/udp/transport.h>
#include <utils.h>

UDPTransport::UDPTransport(const Configuration *config)
    : Transport(config), socket_fd(-1), controller_fd(-1)
{
    evthread_use_pthreads();

    this->event_base = event_base_new();
    if (this->event_base == nullptr) {
        panic("Failed to create new libevent event base");
    }

    switch (config->node_type) {
    case Configuration::NodeType::SERVER:
        register_address(config->node_addresses.at(config->rack_id).at(config->node_id));
        break;
    case Configuration::NodeType::CLIENT:
        register_address(config->client_addresses.at(config->client_id));
        break;
    case Configuration::NodeType::LB:
        register_address(config->lb_address);
        break;
    default:
        panic("Unreachable");
    }
    register_controller();
}

UDPTransport::~UDPTransport()
{
    for (auto event : this->events) {
        event_free(event);
    }

    if (this->socket_fd > 0) {
        close(this->socket_fd);
    }
    if (this->controller_fd > 0) {
        close(this->controller_fd);
    }

    if (this->event_base != nullptr) {
        event_base_free(this->event_base);
    }
}

void UDPTransport::send_message(const Message &msg, const Address &addr)
{
    const UDPAddress &udp_addr = static_cast<const UDPAddress&>(addr);

    if (sendto(this->socket_fd, msg.buf(), msg.len(), 0,
               &udp_addr.saddr, sizeof(udp_addr.saddr)) == -1) {
        printf("Failed to send message\n");
    }
}

void UDPTransport::run(void)
{
    this->transport_thread = new std::thread(&UDPTransport::run_transport, this);
}

void UDPTransport::stop(void)
{
    event_base_loopbreak(this->event_base);
}

void UDPTransport::wait(void)
{
    this->transport_thread->join();
    delete this->transport_thread;
}

void UDPTransport::run_app_threads(Application *app)
{
    std::thread *app_threads[this->config->n_app_threads];

    for (int i = 1; i < this->config->n_app_threads; i++) {
        app_threads[i] = new std::thread(&Application::run_thread, app, i);
    }
    app->run_thread(0);
    for (int i = 1; i < this->config->n_app_threads; i++) {
        app_threads[i]->join();
        delete app_threads[i];
    }
}

void UDPTransport::register_address(const Address *addr)
{
    const UDPAddress *udp_addr = static_cast<const UDPAddress*>(addr);

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
    if (bind(this->socket_fd,
             &udp_addr->saddr,
             sizeof(udp_addr->saddr)) != 0) {
        panic("Failed to bind port");
    }

    add_socket_event(this->socket_fd);
}

void UDPTransport::register_controller()
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

    add_socket_event(this->controller_fd);
}

void UDPTransport::on_readable(int fd)
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
    this->receiver->receive_message(Message(buf, ret, false),
                                    UDPAddress(src_addr),
                                    0); // Currently single threaded transport
}

void UDPTransport::add_socket_event(int fd)
{
    struct event *sock_ev = event_new(this->event_base,
                                      fd,
                                      EV_READ | EV_PERSIST,
                                      socket_callback,
                                      (void *)this);
    if (sock_ev == nullptr) {
        panic("Failed to create new event");
    }

    event_add(sock_ev, nullptr);
    this->events.push_back(sock_ev);
}

void UDPTransport::run_transport(void)
{
    pin_to_core(this->config->transport_core);
    event_base_dispatch(this->event_base);
}

void UDPTransport::socket_callback(evutil_socket_t fd, short what, void *arg)
{
    if (what & EV_READ) {
        UDPTransport *transport = (UDPTransport *)arg;
        transport->on_readable(fd);
    }
}
