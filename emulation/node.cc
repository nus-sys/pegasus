#include <thread>
#include "node.h"

using std::thread;

Node::Node(int id, Transport *transport, Application *app)
{
    assert(transport != nullptr);
    assert(app != nullptr);
    this->id = id;
    this->transport = transport;
    this->app = app;
}

void
Node::run()
{
    // Create one thread which runs the transport event loop.
    thread transport_thread = thread(&Node::run_transport, this);

    // Run application logic
    this->app->run();

    // Wait for transport thread
    transport_thread.join();
}

void
Node::run_transport()
{
    this->transport->run();
}
