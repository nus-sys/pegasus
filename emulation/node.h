#ifndef _NODE_H_
#define _NODE_H_

#include <thread>
#include <list>

#include <transport.h>
#include <configuration.h>
#include <application.h>

class Node {
public:
    Node(const Configuration *config, Transport *transport);
    ~Node();

    void register_app(Application *app);
    void run();

private:
    const Configuration *config;
    Transport *transport;
    Application *app;
};

#endif /* _NODE_H_ */
