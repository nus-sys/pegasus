#ifndef __NODE_H__
#define __NODE_H__

#include <thread>
#include "transport.h"
#include "configuration.h"
#include "application.h"

class Node {
public:
    Node(const Configuration *config);
    ~Node();

    void register_app(Application *app);
    void run(int duration);
    void test_run(); // For testing
    void test_stop(); // For testing

private:
    void run_transport();

    const Configuration *config;
    Transport *transport;
    Application *app;
    std::thread transport_thread;
};

#endif /* __NODE_H__ */
