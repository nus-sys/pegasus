#ifndef __TRANSPORT_H__
#define __TRANSPORT_H__

#include <string>
#include <list>
#include <event2/event.h>
#include <google/protobuf/message.h>
#include "configuration.h"

class Transport;

class TransportReceiver {
public:
    virtual ~TransportReceiver() {};
    void register_transport(Transport *transport)
    {
        this->transport = transport;
    }
    virtual void receive_message(const std::string &msg,
                                 const sockaddr &src_addr) = 0;
protected:
    Transport *transport;
};

class Transport {
public:
    Transport(const Configuration *config);
    ~Transport();

    void register_receiver(TransportReceiver *receiver);
    void run();
    void stop();
    void send_message(const std::string &msg, const sockaddr &addr);
    void send_message_to_addr(const std::string &msg, const NodeAddress &addr);
    void send_message_to_node(const std::string &msg, int dst_node_id);
    void send_message_to_controller(const std::string &msg);

private:
    void register_address(const NodeAddress &node_addr);
    void listen_on_controller();
    static void socket_callback(evutil_socket_t fd, short what, void *arg);
    static void signal_callback(evutil_socket_t fd, short what, void *arg);
    void on_readable(int fd);

    const int SOCKET_BUF_SIZE = 1024 * 1024; // 1MB buffer size

    const Configuration *config;
    TransportReceiver *receiver;
    struct event_base *event_base;
    std::list<struct event*> events;
    int socket_fd;
    int controller_fd;
};

#endif /* __TRANSPORT_H__ */
