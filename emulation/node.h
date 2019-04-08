#ifndef __NODE_H__
#define __NODE_H__

#include <thread>
#include <list>
#include "transport.h"
#include "configuration.h"
#include "application.h"

class Node {
public:
    Node(const Configuration *config);
    ~Node();

    void register_app(Application *app);
    void run(int duration);

private:
    void run_transport();

    const Configuration *config;
    Transport *transport;
    Application *app;
    std::list<TransportEventBase*> transport_ebs;
    std::list<std::thread*> transport_threads;
};

#endif /* __NODE_H__ */
