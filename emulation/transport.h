#ifndef __TRANSPORT_H__
#define __TRANSPORT_H__

#include <string>
#include <list>
#include <event2/event.h>
#include <google/protobuf/message.h>
#include "configuration.h"

class TransportReceiver {
public:
    virtual ~TransportReceiver() {};
    virtual void receive_message(const std::string &msg,
                                 const sockaddr &src_addr) = 0;
};

class Transport {
public:
    Transport();
    ~Transport();

    void register_address(TransportReceiver *receiver,
                          Configuration *config,
                          const NodeAddress &node_addr);
    void register_node(TransportReceiver *receiver,
                       Configuration *config,
                       int node_id);
    void register_router(TransportReceiver *receiver,
                         Configuration *config);
    void run();
    void stop();
    void send_message(const std::string &msg, const sockaddr &addr);
    void send_message_to_node(const std::string &msg, int dst_node_id);
    void send_message_to_router(const std::string &msg);

private:
    static void socket_callback(evutil_socket_t fd, short what, void *arg);
    static void signal_callback(evutil_socket_t fd, short what, void *arg);
    void on_readable(int fd);

    TransportReceiver *receiver;
    Configuration *config;
    event_base *event_base;
    std::list<struct event*> events;
    int socket_fd;
};

#endif /* __TRANSPORT_H__ */