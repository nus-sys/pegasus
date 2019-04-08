#include "logger.h"
#include "node.h"

Node::Node(const Configuration *config)
    : config(config)
{
    this->transport = new Transport(config);
}

Node::~Node()
{
    for (TransportEventBase *eb : this->transport_ebs) {
        delete eb;
    }
    for (std::thread *thread : this->transport_threads) {
        delete thread;
    }
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
    // Create threads which run the transport event loop.
    for (int i = 0; i < this->config->n_transport_threads; i++) {
        this->transport_threads.push_back(new std::thread(&Node::run_transport, this));
    }

    // Run application logic
    this->app->run(duration);

    if (this->config->terminating) {
        // Stop transport event base now
        for (TransportEventBase *eb : this->transport_ebs) {
            eb->stop();
        }
    }

    // Wait for transport threads
    for (std::thread *thread : this->transport_threads) {
        thread->join();
    }
}

void
Node::run_transport()
{
    TransportEventBase *eb = new TransportEventBase(this->transport);
    this->transport_ebs.push_back(eb);
    eb->run();
}
