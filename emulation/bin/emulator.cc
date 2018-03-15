#include <unistd.h>
#include "configuration.h"
#include "node.h"
#include "transport.h"
#include "memcachekv/server.h"
#include "memcachekv/client.h"

enum Mode {
    CLIENT,
    SERVER,
    UNKNOWN
};

int main(int argc, char *argv[])
{
    int opt;
    Mode mode = UNKNOWN;

    while ((opt = getopt(argc, argv, "m:")) != -1) {
        switch (opt) {
        case 'm': {
            if (strcmp(optarg, "client") == 0) {
                mode = CLIENT;
            } else if (strcmp(optarg, "server") == 0) {
                mode = SERVER;
            }
            break;
        }
        default:
            printf("Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (mode == UNKNOWN) {
        printf("Option -m <client/server> required\n");
        exit(1);
    }

    std::map<int, NodeAddress> addrs;
    addrs[0] = NodeAddress("localhost", "12345");
    NodeAddress router_addr("localhost", "54321");
    Configuration config(1, addrs, router_addr);
    Transport transport;
    memcachekv::MemcacheKVStats stats;
    memcachekv::KVWorkloadGenerator gen;

    Node *node;
    Application *app;
    switch (mode) {
    case CLIENT: {
        app = new memcachekv::Client(&transport, &stats, &gen);
        transport.register_node(app, &config, -1);
        node = new Node(-1, &transport, app, true);
        break;
    }
    case SERVER: {
        app = new memcachekv::Server(&transport, &config);
        transport.register_node(app, &config, 0);
        node = new Node(0, &transport, app, false);
        break;
    }
    default:
        printf("Unknown application mode\n");
        exit(1);
    }

    node->run(1);

    delete node;
    delete app;

    return 0;
}
