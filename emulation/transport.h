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
    void send_message_to_addr(const std::string &msg, const NodeAddress &addr);
    void send_message_to_node(const std::string &msg, int dst_node_id);

private:
    static void socket_callback(evutil_socket_t fd, short what, void *arg);
    static void signal_callback(evutil_socket_t fd, short what, void *arg);
    void on_readable(int fd);

    const int SOCKET_BUF_SIZE = 1024 * 1024; // 1MB buffer size

    TransportReceiver *receiver;
    Configuration *config;
    struct event_base *event_base;
    std::list<struct event*> events;
    int socket_fd;
};

#endif /* __TRANSPORT_H__ */
