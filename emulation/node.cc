#include "logger.h"
#include "node.h"

using std::thread;

Node::Node(int id,
           Transport *transport,
           Application *app,
           bool terminating,
           int app_core,
           int transport_core)
    : transport(transport), app(app), id (id), terminating(terminating),
    app_core(app_core), transport_core(transport_core)
{
}

void
Node::run(int duration)
{
    // Create one thread which runs the transport event loop.
    this->transport_thread = thread(&Node::run_transport, this);

    if (this->app_core >= 0) {
        // Pin app thread to core
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(this->app_core, &set);
        if (sched_setaffinity(0, sizeof(set), &set) != 0) {
            panic("Failed to pin app thread");
        }
    }

    // Run application logic
    this->app->run(duration);

    if (this->terminating) {
        // Stop transport now
        this->transport->stop();
    }

    // Wait for transport thread
    this->transport_thread.join();
}

void
Node::test_run()
{
    // Create one thread which runs the transport event loop.
    this->transport_thread = thread(&Node::run_transport, this);
}

void
Node::test_stop()
{
    this->transport->stop();
    this->transport_thread.join();
}

void
Node::run_transport()
{
    if (this->transport_core >= 0) {
        // Pin transport thread to core
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(this->transport_core, &set);
        if (sched_setaffinity(0, sizeof(set), &set) != 0) {
            panic("Failed to pin transport thread");
        }
    }
    this->transport->run();
}
