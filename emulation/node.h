#ifndef __NODE_H__
#define __NODE_H__

#include "transport.h"
#include "configuration.h"
#include "application.h"

class Node {
public:
    Node(int id, Transport *transport, Application *app, bool terminating);
    ~Node() {};

    void run(int duration);

private:
    void run_transport();

    Transport *transport;
    Application *app;
    int id;
    bool terminating;
};

#endif /* __NODE_H__ */
