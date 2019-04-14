#include <unistd.h>
#include <fstream>
#include <sched.h>
#include <signal.h>

#include "node.h"
#include "transport.h"
#include "memcachekv/config.h"
#include "memcachekv/message.h"
#include "memcachekv/server.h"
#include "memcachekv/client.h"
#include "memcachekv/controller.h"
#include "memcachekv/decrementor.h"

enum NodeMode {
    CLIENT,
    SERVER,
    CONTROLLER,
    DECREMENTOR,
    UNKNOWN
};

enum CodecMode {
    PROTOBUF,
    WIRE
};

void sigint_handler(int param)
{
    printf("Received INT signal\n");
    exit(1);
}

void sigterm_handler(int param)
{
    printf("Received TERM signal\n");
    exit(1);
}

int main(int argc, char *argv[])
{
    int opt;
    NodeMode mode = UNKNOWN;
    int n_transport_threads = 1, value_len = 256, mean_interval = 1000, nkeys = 1000, duration = 1, rack_id = -1, node_id = -1, num_racks = 1, num_nodes = 1, proc_latency = 0, dec_interval = 1000, n_dec = 1, num_rkeys = 32, interval = 0, d_interval = 1000000, d_nkeys = 100;
    float get_ratio = 0.5, put_ratio = 0.5, alpha = 0.5;
    bool report_load = false;
    const char *keys_file_path = nullptr, *config_file_path = nullptr, *stats_file_path = nullptr, *interval_file_path = nullptr;
    std::deque<std::string> keys;
    memcachekv::KeyType key_type = memcachekv::UNIFORM;
    memcachekv::MemcacheKVConfig::NodeConfigMode node_config_mode = memcachekv::MemcacheKVConfig::STATIC;
    CodecMode codec_mode = WIRE;
    memcachekv::DynamismType d_type = memcachekv::DynamismType::NONE;

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigterm_handler);
    //std::srand(unsigned(std::time(0)));

    while ((opt = getopt(argc, argv, "a:b:c:d:e:f:g:i:j:l:m:n:p:r:s:t:v:w:x:y:z:A:B:C:D:E:F:G:H:")) != -1) {
        switch (opt) {
        case 'a': {
            alpha = stof(std::string(optarg));
            break;
        }
        case 'b': {
            n_transport_threads = stoi(std::string(optarg));
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
            int input = stoi(std::string(optarg));
            report_load = input != 0;
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
            } else if (strcmp(optarg, "controller") == 0) {
                mode = CONTROLLER;
            } else if (strcmp(optarg, "decrementor") == 0) {
                mode = DECREMENTOR;
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
        case 'p': {
            put_ratio = stof(std::string(optarg));
            break;
        }
        case 'r': {
            rack_id = stoi(std::string(optarg));
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
            } else if (strcmp(optarg, "netcache") == 0) {
                node_config_mode = memcachekv::MemcacheKVConfig::NETCACHE;
            } else {
                printf("Unknown node config mode %s\n", optarg);
                exit(1);
            }
            break;
        }
        case 'x': {
            num_racks = stoi(std::string(optarg));
            if (num_racks < 1) {
                printf("Number of racks should be > 0\n");
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
        case 'A': {
            dec_interval = stoi(std::string(optarg));
            if (dec_interval < 1) {
                printf("Decrement interval should be > 0\n");
                exit(1);
            }
            break;
        }
        case 'B': {
            n_dec = stoi(std::string(optarg));
            if (n_dec < 1) {
                printf("Decrement amount should be > 0\n");
                exit(1);
            }
            break;
        }
        case 'C': {
            num_rkeys = stoi(std::string(optarg));
            if (num_rkeys < 0) {
                printf("num_rkeys should be >= 0\n");
                exit(1);
            }
            break;
        }
        case 'D': {
            interval = stoi(std::string(optarg));
            if (interval < 0) {
                printf("interval should be >= 0\n");
                exit(1);
            }
            break;
        }
        case 'E': {
            interval_file_path = optarg;
            break;
        }
        case 'F': {
            if (strcmp(optarg, "none") == 0) {
                d_type = memcachekv::DynamismType::NONE;
            } else if (strcmp(optarg, "hotin") == 0) {
                d_type = memcachekv::DynamismType::HOTIN;
            } else if (strcmp(optarg, "random") == 0) {
                d_type = memcachekv::DynamismType::RANDOM;
            } else {
                printf("Unknown dynamism type %s\n", optarg);
                exit(1);
            }
            break;
        }
        case 'G': {
            d_interval = stoi(std::string(optarg));
            d_interval *= 1000000;
            break;
        }
        case 'H': {
            d_nkeys = stoi(std::string(optarg));
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
    node_config.n_transport_threads = n_transport_threads;
    node_config.num_nodes = num_nodes;
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
        if (node_config_mode == memcachekv::MemcacheKVConfig::ROUTER) {
            codec = new memcachekv::WireCodec(true);
        } else if (node_config_mode == memcachekv::MemcacheKVConfig::STATIC) {
            codec = new memcachekv::WireCodec(false);
        } else if (node_config_mode == memcachekv::MemcacheKVConfig::NETCACHE) {
            codec = new memcachekv::NetcacheCodec();
        }
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
        stats = new memcachekv::MemcacheKVStats(stats_file_path, interval, interval_file_path);
        gen = new memcachekv::KVWorkloadGenerator(&keys,
                                                  value_len,
                                                  get_ratio,
                                                  put_ratio,
                                                  mean_interval,
                                                  alpha,
                                                  key_type,
                                                  d_type,
                                                  d_interval,
                                                  d_nkeys);

        node_config.rack_id = -1;
        node_config.node_id = -1;
        node_config.terminating = true;
        app = new memcachekv::Client(&node_config, stats, gen, codec, node_id);
        break;
    }
    case SERVER: {
        if (rack_id < 0) {
            printf("server requires argument '-r <rack id>'\n");
            exit(1);
        }
        if (node_id < 0) {
            printf("server requires argument '-e <node id>'\n");
            exit(1);
        }
        node_config.rack_id = rack_id;
        node_config.node_id = node_id;
        node_config.terminating = false;
        std::string default_value = std::string(value_len, 'v');
        app = new memcachekv::Server(&node_config, codec, ctrl_codec, proc_latency, default_value, report_load);
        break;
    }
    case CONTROLLER: {
        node_config.rack_id = -1;
        node_config.node_id = -1;
        node_config.terminating = true;
        memcachekv::ControllerMessage msg;
        msg.type = memcachekv::ControllerMessage::Type::RESET_REQ;
        msg.reset_req.num_nodes = num_nodes;
        msg.reset_req.num_rkeys = num_rkeys;
        app = new memcachekv::Controller(&node_config, msg);
        break;
    }
    case DECREMENTOR: {
        node_config.rack_id = -1;
        node_config.node_id = -1;
        node_config.terminating = false;
        app = new memcachekv::Decrementor(&node_config, dec_interval, n_dec);
        break;
    }
    default:
        printf("Unknown application mode\n");
        exit(1);
    }

    node = new Node(&node_config);
    node->register_app(app);
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
