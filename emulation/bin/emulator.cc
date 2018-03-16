#include <unistd.h>
#include <fstream>
#include "configuration.h"
#include "node.h"
#include "transport.h"
#include "memcachekv/server.h"
#include "memcachekv/client.h"

enum AppMode {
    CLIENT,
    SERVER,
    UNKNOWN
};

int main(int argc, char *argv[])
{
    int opt;
    AppMode mode = UNKNOWN;
    int value_len = 256, mean_interval = 1000, nkeys = 1000, duration = 1;
    float get_ratio = 0.5, put_ratio = 0.5, alpha = 0.5;
    const char *keys_file_path;
    std::vector<std::string> keys;
    memcachekv::KeyType key_type = memcachekv::UNIFORM;

    while ((opt = getopt(argc, argv, "a:d:f:g:i:m:n:p:t:v:")) != -1) {
        switch (opt) {
        case 'a': {
            alpha = stof(std::string(optarg));
            break;
        }
        case 'd': {
            duration = stoi(std::string(optarg));
            break;
        }
        case 'f': {
            keys_file_path = optarg;
            break;
        }
        case 'g': {
            get_ratio = stof(std::string(optarg));
            break;
        }
        case 'i': {
            mean_interval = stoi(std::string(optarg));
            break;
        }
        case 'm': {
            if (strcmp(optarg, "client") == 0) {
                mode = CLIENT;
            } else if (strcmp(optarg, "server") == 0) {
                mode = SERVER;
            }
            break;
        }
        case 'n': {
            nkeys = stoi(std::string(optarg));
            break;
        }
        case 'p': {
            put_ratio = stof(std::string(optarg));
            break;
        }
        case 't': {
            if (strcmp(optarg, "unif") == 0) {
                key_type = memcachekv::KeyType::UNIFORM;
            } else if (strcmp(optarg, "zipf") == 0) {
                key_type = memcachekv::KeyType::ZIPF;
            } else {
                printf("Unknown key type %s\n", optarg);
                exit(1);
            }
            break;
        }
        case 'v': {
            value_len = stoi(std::string(optarg));
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
    Node *node;
    Application *app;
    memcachekv::MemcacheKVStats *stats;
    memcachekv::KVWorkloadGenerator *gen;

    switch (mode) {
    case CLIENT: {
        // Read in all keys
        std::ifstream in;
        in.open(keys_file_path);
        if (!in) {
            printf("Failed to read keys from %s\n", keys_file_path);
            exit(1);
        }
        std::string key;
        for (int i = 0; i < nkeys; i++) {
            getline(in, key);
            keys.push_back(key);
        }

        stats = new memcachekv::MemcacheKVStats();
        gen = new memcachekv::KVWorkloadGenerator(&keys,
                                                  value_len,
                                                  get_ratio,
                                                  put_ratio,
                                                  mean_interval,
                                                  alpha,
                                                  key_type);

        app = new memcachekv::Client(&transport, stats, gen);
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

    node->run(duration);

    delete node;
    delete app;
    if (mode == CLIENT) {
        delete gen;
        delete stats;
    }

    return 0;
}
