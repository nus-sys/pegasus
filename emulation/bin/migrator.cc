#include <unistd.h>
#include <fstream>
#include <sched.h>
#include "node.h"
#include "transport.h"
#include "memcachekv/config.h"
#include "memcachekv/message.h"
#include "memcachekv/migration_server.h"
#include "memcachekv/migration_client.h"

enum NodeMode {
    CLIENT,
    SERVER,
    UNKNOWN
};

enum CodecMode {
    PROTOBUF,
    WIRE
};

int main(int argc, char *argv[])
{
    int opt;
    NodeMode mode = UNKNOWN;
    int value_len = 256, key_len = 64, nkeys = 1, duration = 1, app_core = -1, transport_core = -1, interval = 0;
    const char *config_file_path = nullptr, *stats_file_path = nullptr;
    CodecMode codec_mode = WIRE;

    while ((opt = getopt(argc, argv, "c:d:k:m:n:o:r:s:v:y:")) != -1) {
        switch (opt) {
        case 'c': {
            config_file_path = optarg;
            break;
        }
        case 'd': {
            duration = stoi(std::string(optarg));
            break;
        }
        case 'k': {
            key_len = stoi(std::string(optarg));
            break;
        }
        case 'm': {
            if (strcmp(optarg, "client") == 0) {
                mode = CLIENT;
            } else if (strcmp(optarg, "server") == 0) {
                mode = SERVER;
            } else {
                printf("Unknown mode %s\n", optarg);
                exit(1);
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
        case 'r': {
            transport_core = stoi(std::string(optarg));
            break;
        }
        case 's': {
            stats_file_path = optarg;
            break;
        }
        case 'v': {
            value_len = stoi(std::string(optarg));
            break;
        }
        case 'y': {
            if (strcmp(optarg, "protobuf") == 0) {
                codec_mode = PROTOBUF;
            } else if (strcmp(optarg, "wire") == 0) {
                codec_mode = WIRE;
            } else {
                printf("Unknown codec mode %s\n", optarg);
                exit(1);
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

    if (config_file_path == nullptr) {
        printf("Option -c <config file> required\n");
        exit(1);
    }

    memcachekv::MemcacheKVConfig node_config(config_file_path, memcachekv::MemcacheKVConfig::STATIC);
    node_config.num_nodes = 1;
    Transport transport;
    Node *node = nullptr;
    Application *app = nullptr;
    Stats *stats = nullptr;
    memcachekv::MessageCodec *codec = nullptr;

    switch (codec_mode) {
    case PROTOBUF: {
        codec = new memcachekv::ProtobufCodec();
        break;
    }
    case WIRE: {
        codec = new memcachekv::WireCodec();
        break;
    }
    default:
        printf("Unknown codec mode %d\n", codec_mode);
        exit(1);
    }

    switch (mode) {
    case CLIENT: {
        stats = new Stats(stats_file_path, interval);
        app = new memcachekv::MigrationClient(&transport, &node_config, codec, stats, nkeys, key_len, value_len);
        transport.register_node(app, &node_config, -1);
        node = new Node(-1, &transport, app, true, app_core, transport_core);
        break;
    }
    case SERVER: {
        app = new memcachekv::MigrationServer(&transport, &node_config, codec);
        transport.register_node(app, &node_config, 0);
        node = new Node(0, &transport, app, false, app_core, transport_core);
        break;
    }
    default:
        printf("Unknown application mode\n");
        exit(1);
    }

    node->run(duration);

    delete node;
    delete app;
    delete codec;
    if (mode == CLIENT) {
        delete stats;
    }

    return 0;
}
