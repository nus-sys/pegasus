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
Node::run()
{
    this->transport->run();

    // Run application logic
    this->app->run();

    if (this->config->terminating) {
        this->transport->stop();
    }

    // Wait for transport to finish
    this->transport->wait();
}
