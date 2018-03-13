#ifndef __NODE_H__
#define __NODE_H__

#include "transport.h"
#include "configuration.h"
#include "application.h"

class Node {
public:
    Node(int id, Transport *transport, Application *app);
    ~Node() {};

    void run();

private:
    void run_transport();

    Transport *transport;
    Application *app;
    int id;
};

#endif /* __NODE_H__ */
