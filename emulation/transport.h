#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_

#include <string>
#include <list>

#include <configuration.h>

class Transport;

class TransportReceiver {
public:
    virtual ~TransportReceiver() = 0;
    void register_transport(Transport *transport);
    virtual void receive_message(const std::string &msg,
                                 const Address &addr) = 0;
protected:
    Transport *transport;
};

class Transport {
public:
    Transport(const Configuration *config);
    virtual ~Transport() = 0;

    void register_receiver(TransportReceiver *receiver);
    void send_message_to_node(const std::string &msg, int rack_id, int node_id);
    void send_message_to_local_node(const std::string &msg, int node_id);
    void send_message_to_router(const std::string &msg);
    void send_message_to_controller(const std::string &msg, int rack_id);

    virtual void send_message(const std::string &msg, const Address &addr) = 0;
    virtual void run(void) = 0;
    virtual void stop(void) = 0;
    virtual void wait(void) = 0;

protected:
    const Configuration *config;
    TransportReceiver *receiver;
};

#endif /* _TRANSPORT_H_ */
