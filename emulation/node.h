#ifndef __NODE_H__
#define __NODE_H__

#include <thread>
#include "transport.h"
#include "configuration.h"
#include "application.h"

class Node {
public:
    Node(int id,
         Transport *transport,
         Application *app,
         bool terminating,
         int app_core = -1,
         int transport_core = -1);
    ~Node() {};

    void run(int duration);
    void test_run(); // For testing
    void test_stop(); // For testing

private:
    void run_transport();

    Transport *transport;
    Application *app;
    int id;
    bool terminating;
    int app_core;
    int transport_core;
    std::thread transport_thread;
};

#endif /* __NODE_H__ */
