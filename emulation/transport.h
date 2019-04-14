#ifndef __TRANSPORT_H__
#define __TRANSPORT_H__

#include <string>
#include <list>
#include <event2/event.h>
#include <google/protobuf/message.h>
#include "configuration.h"

class Transport;
class TransportEventBase;

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
    void send_message(const std::string &msg, const sockaddr &addr);
    void send_message_to_node(const std::string &msg, int rack_id, int node_id);
    void send_message_to_local_node(const std::string &msg, int node_id);
    void send_message_to_router(const std::string &msg);
    void send_message_to_controller(const std::string &msg);

private:
    void send_message_to_addr(const std::string &msg, const NodeAddress &addr);
    void register_address(const NodeAddress &node_addr);
    void on_readable(int fd);

    const int SOCKET_BUF_SIZE = 1024 * 1024; // 1MB buffer size

    const Configuration *config;
    TransportReceiver *receiver;
    int socket_fd;

    friend class TransportEventBase;
};

class TransportEventBase {
public:
    TransportEventBase(Transport *transport);
    ~TransportEventBase();

    void run();
    void stop();

private:
    void add_socket_event(int fd);
    static void socket_callback(evutil_socket_t fd, short what, void *arg);
    static void signal_callback(evutil_socket_t fd, short what, void *arg);

    Transport *transport;
    struct event_base *event_base;
    std::list<struct event*> events;
};

#endif /* __TRANSPORT_H__ */
