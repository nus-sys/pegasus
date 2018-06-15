#include <gtest/gtest.h>
#include <vector>
#include "node.h"
#include "memcachekv/client.h"
#include "memcachekv/server.h"

namespace memcachekv {

class TestConfig : public Configuration {
public:
    using Configuration::Configuration;
    ~TestConfig() {};

    const NodeAddress& key_to_address(const std::string &key) override
    {
        int sum = 0;
        for (auto c : key) {
            sum += int(c);
        }
        return this->addresses[sum % this->num_nodes];
    }
};

class MemcacheKVTest : public ::testing::Test
{
protected:
    std::vector<Node *> server_nodes;
    std::vector<Transport *> server_transports;
    std::vector<Server *> servers;
    Node *client_node;
    Transport *client_transport;
    Client *client;
    Configuration *config;
    MessageCodec *codec;
    MemcacheKVStats *stats;
    const int N_SERVERS = 4;

    virtual void SetUp() {
        // Create configuration
        /*
        std::vector<NodeAddress> node_addresses;
        for (int i = 0; i < N_SERVERS; i++) {
            node_addresses.push_back(NodeAddress(std::string("localhost"), std::to_string(12345+i)));
        }
        this->config = new TestConfig(node_addresses, NodeAddress());
        this->codec = new WireCodec();
        this->stats = new MemcacheKVStats();
        // Create servers
        for (int i = 0; i < N_SERVERS; i++) {
            Transport *transport = new Transport();
            Server *app = new Server(transport, this->config, this->codec);
            transport->register_node(app, this->config, i);
            Node *node = new Node(i, transport, app, false);
            this->server_transports.push_back(transport);
            this->servers.push_back(app);
            this->server_nodes.push_back(node);
            node->test_run();
        }
        // Create clients
        this->client_transport = new Transport();
        this->client = new Client(this->client_transport, this->config, this->stats, nullptr, this->codec, 0);
        this->client_transport->register_node(this->client, this->config, -1);
        this->client_node = new Node(-1, this->client_transport, this->client, false);
        this->client_node->test_run();
        */
    }
    virtual void TearDown() {
        /*
        this->client_node->test_stop();
        delete this->client_node;
        delete this->client;
        delete this->client_transport;
        for (int i = 0; i < N_SERVERS; i++) {
            this->server_nodes[i]->test_stop();
            delete this->server_nodes[i];
            delete this->servers[i];
            delete this->server_transports[i];
        }
        delete this->stats;
        delete this->codec;
        delete this->config;
        */
    }
};

TEST_F(MemcacheKVTest, basic)
{
    EXPECT_EQ(this->N_SERVERS, 4);
}

} // namespace memcachekv
