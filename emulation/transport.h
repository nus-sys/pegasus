#ifndef _TRANSPORT_H_
#define _TRANSPORT_H_

#include <string>
#include <list>

#include <configuration.h>

class Transport;
class Application;

class Message {
public:
    Message();
    Message(void *buf, size_t len, bool dealloc);
    Message(const std::string &str);
    ~Message();

    const void *buf() const;
    size_t len() const;
    void set_message(void *buf, size_t len, bool dealloc);

private:
    void *buf_;
    size_t len_;
    bool dealloc_;
};

class TransportReceiver {
public:
    virtual ~TransportReceiver() = 0;
    void register_transport(Transport *transport);
    virtual void receive_message(const Message &msg,
                                 const Address &addr,
                                 int tid) = 0;
protected:
    Transport *transport;
};

class Transport {
public:
    Transport(const Configuration *config);
    virtual ~Transport() = 0;

    void register_receiver(TransportReceiver *receiver);
    void send_message_to_node(const Message &msg, int rack_id, int node_id);
    void send_message_to_local_node(const Message &msg, int node_id);
    void send_message_to_router(const Message &msg);
    void send_message_to_controller(const Message &msg, int rack_id);

    virtual void send_message(const Message &msg, const Address &addr) = 0;
    virtual void run(void) = 0;
    virtual void stop(void) = 0;
    virtual void wait(void) = 0;
    virtual void run_app_threads(Application *app) = 0;

protected:
    const Configuration *config;
    TransportReceiver *receiver;
};

#endif /* _TRANSPORT_H_ */
