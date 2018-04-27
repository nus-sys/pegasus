#include <unistd.h>
#include <fstream>
#include <sched.h>
#include "node.h"
#include "transport.h"
#include "memcachekv/config.h"
#include "memcachekv/message.h"
#include "memcachekv/server.h"
#include "memcachekv/client.h"
#include "memcachekv/router.h"
#include "memcachekv/controller.h"

enum NodeMode {
    CLIENT,
    SERVER,
    ROUTER,
    CONTROLLER,
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
    int value_len = 256, mean_interval = 1000, nkeys = 1000, duration = 1, node_id = -1, num_nodes = 1, proc_latency = 0,
        app_core = -1, transport_core = -1, dscp = -1;
    float get_ratio = 0.5, put_ratio = 0.5, alpha = 0.5;
    const char *keys_file_path = nullptr, *config_file_path = nullptr, *stats_file_path = nullptr;
    memcachekv::ControllerResetMessage::LBType lb_type = memcachekv::ControllerResetMessage::LBType::STATIC;
    std::vector<std::string> keys;
    memcachekv::KeyType key_type = memcachekv::UNIFORM;
    memcachekv::MemcacheKVConfig::NodeConfigMode node_config_mode = memcachekv::MemcacheKVConfig::STATIC;
    memcachekv::RouterConfig::RouterConfigMode router_config_mode = memcachekv::RouterConfig::STATIC;
    CodecMode codec_mode = PROTOBUF;

    while ((opt = getopt(argc, argv, "a:c:d:e:f:g:i:j:k:l:m:n:o:p:r:s:t:v:w:x:y:z:")) != -1) {
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
        case 'j': {
            dscp = stoi(std::string(optarg));
            break;
        }
        case 'k': {
            if (strcmp(optarg, "static") == 0) {
                lb_type = memcachekv::ControllerResetMessage::LBType::STATIC;
            } else if (strcmp(optarg, "iload") == 0) {
                lb_type = memcachekv::ControllerResetMessage::LBType::ILOAD;
            } else if (strcmp(optarg, "pload") == 0) {
                lb_type = memcachekv::ControllerResetMessage::LBType::PLOAD;
            } else if (strcmp(optarg, "ipload") == 0) {
                lb_type = memcachekv::ControllerResetMessage::LBType::IPLOAD;
            }
            break;
        }
        case 'l': {
            proc_latency = stoi(std::string(optarg));
            break;
        }
        case 'm': {
            if (strcmp(optarg, "client") == 0) {
                mode = CLIENT;
            } else if (strcmp(optarg, "server") == 0) {
                mode = SERVER;
            } else if (strcmp(optarg, "router") == 0) {
                mode = ROUTER;
            } else if (strcmp(optarg, "controller") == 0) {
                mode = CONTROLLER;
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
        case 'w': {
            if (strcmp(optarg, "static") == 0) {
                node_config_mode = memcachekv::MemcacheKVConfig::STATIC;
            } else if (strcmp(optarg, "router") == 0) {
                node_config_mode = memcachekv::MemcacheKVConfig::ROUTER;
            } else {
                printf("Unknown node config mode %s\n", optarg);
                exit(1);
            }
            break;
        }
        case 'x': {
            if (strcmp(optarg, "static") == 0) {
                router_config_mode = memcachekv::RouterConfig::STATIC;
            } else {
                printf("Unknown router config mode %s\n", optarg);
                exit(1);
            }
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
        case 'z': {
            num_nodes = stoi(std::string(optarg));
            if (num_nodes < 1) {
                printf("Number of nodes should be > 0\n");
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

    memcachekv::MemcacheKVConfig node_config(config_file_path, node_config_mode);
    memcachekv::RouterConfig router_config(config_file_path, router_config_mode);
    node_config.num_nodes = num_nodes;
    router_config.num_nodes = num_nodes;
    Transport transport(dscp);
    Node *node = nullptr;
    Application *app = nullptr;
    memcachekv::MemcacheKVStats *stats = nullptr;
    memcachekv::KVWorkloadGenerator *gen = nullptr;
    memcachekv::MessageCodec *codec = nullptr;
    memcachekv::ControllerCodec *ctrl_codec = new memcachekv::ControllerCodec();

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

        if (node_id < 0) {
            printf("client requires argument '-e <client id>'\n");
            exit(1);
        }
        stats = new memcachekv::MemcacheKVStats(stats_file_path);
        gen = new memcachekv::KVWorkloadGenerator(&keys,
                                                  value_len,
                                                  get_ratio,
                                                  put_ratio,
                                                  mean_interval,
                                                  alpha,
                                                  key_type);

        app = new memcachekv::Client(&transport, &node_config, stats, gen, codec, node_id);
        transport.register_node(app, &node_config, -1);
        node = new Node(-1, &transport, app, true, app_core, transport_core);
        break;
    }
    case SERVER: {
        if (node_id < 0) {
            printf("server requires argument '-e <node id>'\n");
            exit(1);
        }
        app = new memcachekv::Server(&transport, &node_config, codec, ctrl_codec, proc_latency);
        transport.register_node(app, &node_config, node_id);
        node = new Node(node_id, &transport, app, false, app_core, transport_core);
        break;
    }
    case ROUTER: {
        app = new memcachekv::Router(&transport, &router_config, codec);
        transport.register_router(app, &router_config);
        node = new Node(-1, &transport, app, false, app_core, transport_core);
        break;
    }
    case CONTROLLER: {
        memcachekv::ControllerMessage msg;
        msg.type = memcachekv::ControllerMessage::Type::RESET;
        msg.reset.num_nodes = num_nodes;
        msg.reset.lb_type = lb_type;
        app = new memcachekv::Controller(&transport, &node_config, msg);
        transport.register_node(app, &node_config, -1);
        node = new Node(-1, &transport, app, true, app_core, transport_core);
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
        delete gen;
        delete stats;
    }

    return 0;
}
