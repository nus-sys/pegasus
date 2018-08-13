#include "node.h"
#include "transport.h"
#include "memcachekv/config.h"
#include "memcachekv/message.h"
#include "memcachekv/cli_client.h"

using std::string;
using namespace memcachekv;

int main(int argc, char *argv[])
{
    if (argc < 4) {
        printf("usage: cli <config_file> <op_type> <key>\n");
        exit(1);
    }
    const char *config_file_path = argv[1];
    Operation op;
    op.op_type = static_cast<Operation::Type>(stoi(string(argv[2])));
    op.key = string(argv[3]);
    op.value = op.key;

    MemcacheKVConfig config(config_file_path, MemcacheKVConfig::ROUTER);
    WireCodec codec(true);
    Transport transport;
    CLIClient cli(&transport, &config, &codec, op);
    transport.register_node(&cli, &config, -1);
    Node node(-1, &transport, &cli, false);

    node.run(0);

    return 0;
}
