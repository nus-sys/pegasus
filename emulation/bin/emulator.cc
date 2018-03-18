#include <unistd.h>
#include <fstream>
#include <sched.h>
#include "configuration.h"
#include "node.h"
#include "transport.h"
#include "memcachekv/server.h"
#include "memcachekv/client.h"

enum NodeMode {
    CLIENT,
    SERVER,
    UNKNOWN
};

int main(int argc, char *argv[])
{
    int opt;
    NodeMode mode = UNKNOWN;
    int value_len = 256, mean_interval = 1000, nkeys = 1000, duration = 1, node_id = -1, app_core = -1,
        transport_core = -1;
    float get_ratio = 0.5, put_ratio = 0.5, alpha = 0.5;
    const char *keys_file_path = nullptr, *config_file_path = nullptr, *stats_file_path = nullptr;
    std::vector<std::string> keys;
    memcachekv::KeyType key_type = memcachekv::UNIFORM;

    while ((opt = getopt(argc, argv, "a:c:d:e:f:g:i:m:n:o:p:r:s:t:v:")) != -1) {
        switch (opt) {
        case 'a': {
            alpha = stof(std::string(optarg));
            break;
        }
        case 'c': {
            config_file_path = optarg;
            break;
        }
        case 'd': {
            duration = stoi(std::string(optarg));
            break;
        }
        case 'e': {
            node_id = stoi(std::string(optarg));
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
        case 'o': {
            app_core = stoi(std::string(optarg));
            break;
        }
        case 'p': {
            put_ratio = stof(std::string(optarg));
            break;
        }
        case 'r': {
            transport_core = stoi(std::string(optarg));
            break;
        }
        case 's': {
            stats_file_path = optarg;
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

    if (config_file_path == nullptr) {
        printf("Option -c <config file> required\n");
        exit(1);
    }

    Configuration config(config_file_path);
    Transport transport;
    Node *node = nullptr;
    Application *app = nullptr;
    memcachekv::MemcacheKVStats *stats = nullptr;
    memcachekv::KVWorkloadGenerator *gen = nullptr;

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
        in.close();

        stats = new memcachekv::MemcacheKVStats(stats_file_path);
        gen = new memcachekv::KVWorkloadGenerator(&keys,
                                                  value_len,
                                                  get_ratio,
                                                  put_ratio,
                                                  mean_interval,
                                                  alpha,
                                                  key_type);

        app = new memcachekv::Client(&transport, stats, gen);
        transport.register_node(app, &config, -1);
        node = new Node(-1, &transport, app, true, app_core, transport_core);
        break;
    }
    case SERVER: {
        if (node_id < 0) {
            printf("server requires argument '-e <node id>'\n");
            exit(1);
        }
        app = new memcachekv::Server(&transport, &config);
        transport.register_node(app, &config, node_id);
        node = new Node(node_id, &transport, app, false, app_core, transport_core);
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
