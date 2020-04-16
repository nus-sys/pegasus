#ifndef _UDP_TRANSPORT_H_
#define _UDP_TRANSPORT_H_

#include <thread>
#include <list>
#include <event2/util.h>

#include <transport.h>

class TransportEventBase;

class UDPTransport : public Transport {
public:
    UDPTransport(const Configuration *config);
    ~UDPTransport();

    virtual void send_message(const Message &msg, const Address &addr) override final;
    virtual void run(void) override final;
    virtual void stop(void) override final;
    virtual void wait(void) override final;
    virtual void run_app_threads(Application *app) override final;

private:
    void register_address(const Address *addr);
    void register_controller(void);
    void on_readable(int fd);
    void add_socket_event(int fd);
    void run_transport(void);
    static void socket_callback(evutil_socket_t fd, short what, void *arg);

    const int SOCKET_BUF_SIZE = 1024 * 1024; // 1MB buffer size

    int socket_fd;
    int controller_fd;
    std::thread *transport_thread;
    struct event_base *event_base;
    std::list<struct event *> events;
};

#endif /* __UDP_TRANSPORT_H__ */
