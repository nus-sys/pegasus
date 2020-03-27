#include <logger.h>
#include <node.h>
#include <transport.h>
#include <transports/udp/configuration.h>
#include <transports/udp/transport.h>
#include <apps/memcachekv/message.h>
#include <apps/memcachekv/cli_client.h>

using std::string;
using namespace memcachekv;

int main(int argc, char *argv[])
{
    if (argc < 3) {
        panic("usage: cli <config_file> <node_type> <op_type> <key> (<value>)\n");
    }
    const char *config_file_path = argv[1];
    int node_type = stoi(string(argv[2]));

    MessageCodec *codec;
    switch (node_type) {
    case 0:
        codec = new WireCodec(true);
        break;
    case 1:
        codec = new WireCodec(false);
        break;
    default:
        codec = new NetcacheCodec();
        break;
    }
    Configuration *config = new UDPConfiguration();
    config->load_from_file(config_file_path);
    config->n_transport_threads = 1;
    config->rack_id = -1;
    config->client_id = 0;
    config->is_server = false;
    config->terminating = true;
    Transport *transport = new UDPTransport(config);
    CLIClient cli(config, codec);
    Node node(config, transport);
    node.register_app(&cli);

    node.run(0);

    delete transport;
    delete config;
    delete codec;

    return 0;
}
