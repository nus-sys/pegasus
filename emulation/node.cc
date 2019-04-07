#include "logger.h"
#include "node.h"

using std::thread;

Node::Node(const Configuration *config)
    : config(config)
{
    this->transport = new Transport(config);
}

Node::~Node()
{
    if (this->transport) {
        delete this->transport;
    }
}

void
Node::register_app(Application *app)
{
    assert(app);
    this->app = app;
    this->transport->register_receiver(app);
    this->app->register_transport(this->transport);
}

void
Node::run(int duration)
{
    // Create one thread which runs the transport event loop.
    this->transport_thread = thread(&Node::run_transport, this);

    // Run application logic
    this->app->run(duration);

    if (this->config->terminating) {
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
    this->transport->run();
}
