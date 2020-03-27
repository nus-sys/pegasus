#include <cassert>

#include <logger.h>
#include <node.h>
#include <utils.h>

Node::Node(const Configuration *config, Transport *transport)
    : config(config), transport(transport)
{
}

Node::~Node()
{
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
    this->transport->run();

    // Pin app thread to core
    pin_to_core(this->config->app_core);

    // Run application logic
    this->app->run(duration);

    if (this->config->terminating) {
        this->transport->stop();
    }

    // Wait for transport to finish
    this->transport->wait();
}
