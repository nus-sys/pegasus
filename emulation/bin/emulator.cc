#include <unistd.h>
#include <fstream>
#include <signal.h>

#include <node.h>
#include <logger.h>
#include <transports/udp/configuration.h>
#include <transports/udp/transport.h>
#include <transports/dpdk/configuration.h>
#include <transports/dpdk/transport.h>
#include <apps/echo/client.h>
#include <apps/echo/server.h>
#include <apps/memcachekv/message.h>
#include <apps/memcachekv/server.h>
#include <apps/memcachekv/client.h>
#include <apps/memcachekv/controller.h>
#include <apps/memcachekv/decrementor.h>
#include <apps/memcachekv/loadbalancer.h>

enum class NodeMode {
    CLIENT,
    SERVER,
    CONTROLLER,
    DECREMENTOR,
    LB,
    UNKNOWN
};

enum class TransportMode {
    UDP,
    DPDK
};

enum class ProtocolMode {
    STATIC,
    PEGASUS,
    NETCACHE
};

enum class AppMode {
    ECHO,
    MEMCACHEKV,
    UNKNOWN
};

void sigint_handler(int param)
{
    info("Received INT signal\n");
    exit(1);
}

void sigterm_handler(int param)
{
    info("Received TERM signal\n");
    exit(1);
}

int main(int argc, char *argv[])
{
    int opt;
    NodeMode node_mode = NodeMode::UNKNOWN;
    ProtocolMode protocol_mode = ProtocolMode::STATIC;
    TransportMode transport_mode = TransportMode::UDP;
    AppMode app_mode = AppMode::UNKNOWN;
    int n_transport_threads = 1, n_app_threads = 1, value_len = 256, mean_interval = 1000, nkeys = 1000, duration = 1, rack_id = -1, node_id = -1, num_racks = 1, num_nodes = 1, proc_latency = 0, dec_interval = 1000, n_dec = 1, num_rkeys = 32, interval = 0, d_interval = 1000000, d_nkeys = 100, target_latency = 100, app_core = 0, transport_core = 1, colocate_id = 0, n_colocate_nodes = 1;
    float get_ratio = 0.5, put_ratio = 0.5, alpha = 0.5;
    bool report_load = false, use_endhost_lb = false, use_flow_api = false;
    const char *keys_file_path = nullptr, *config_file_path = nullptr, *stats_file_path = nullptr, *interval_file_path = nullptr;
    std::deque<std::string> keys;
    memcachekv::KeyType key_type = memcachekv::KeyType::UNIFORM;
    memcachekv::DynamismType d_type = memcachekv::DynamismType::NONE;
    memcachekv::SendMode send_mode = memcachekv::SendMode::FIXED;

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigterm_handler);

    while ((opt = getopt(argc, argv, "a:b:c:d:e:f:g:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E:F:G:H:I:J:K:L:M:N:")) != -1) {
        switch (opt) {
        case 'a': {
            alpha = stof(std::string(optarg));
            break;
        }
        case 'b': {
            use_endhost_lb = stoi(std::string(optarg)) == 1;
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
            report_load = stoi(std::string(optarg))!= 0;
            break;
        }
        case 'k': {
            use_flow_api = stoi(std::string(optarg)) != 0;
            break;
        }
        case 'l': {
            proc_latency = stoi(std::string(optarg));
            break;
        }
        case 'm': {
            if (strcmp(optarg, "client") == 0) {
                node_mode = NodeMode::CLIENT;
            } else if (strcmp(optarg, "server") == 0) {
                node_mode = NodeMode::SERVER;
            } else if (strcmp(optarg, "lb") == 0) {
                node_mode = NodeMode::LB;
            } else if (strcmp(optarg, "controller") == 0) {
                node_mode = NodeMode::CONTROLLER;
            } else if (strcmp(optarg, "decrementor") == 0) {
                node_mode = NodeMode::DECREMENTOR;
            } else {
                panic("Unknown mode %s", optarg);
            }
            break;
        }
        case 'n': {
            nkeys = stoi(std::string(optarg));
            break;
        }
        case 'o': {
            if (strcmp(optarg, "udp") == 0) {
                transport_mode = TransportMode::UDP;
            } else if (strcmp(optarg, "dpdk") == 0) {
                transport_mode = TransportMode::DPDK;
            } else {
                panic("Unknown transport mode %s", optarg);
            }
            break;
        }
        case 'p': {
            put_ratio = stof(std::string(optarg));
            break;
        }
        case 'q': {
            if (strcmp(optarg, "echo") == 0) {
                app_mode = AppMode::ECHO;
            } else if (strcmp(optarg, "kv") == 0) {
                app_mode = AppMode::MEMCACHEKV;
            } else {
                panic("Unknown application %s", optarg);
            }
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
                panic("Unknown key type %s", optarg);
            }
            break;
        }
        case 'u': {
            target_latency = stoi(std::string(optarg));
            break;
        }
        case 'v': {
            value_len = stoi(std::string(optarg));
            break;
        }
        case 'w': {
            if (strcmp(optarg, "static") == 0) {
                protocol_mode = ProtocolMode::STATIC;
            } else if (strcmp(optarg, "pegasus") == 0) {
                protocol_mode = ProtocolMode::PEGASUS;
            } else if (strcmp(optarg, "netcache") == 0) {
                protocol_mode = ProtocolMode::NETCACHE;
            } else {
                panic("Unknown node config mode %s", optarg);
            }
            break;
        }
        case 'x': {
            num_racks = stoi(std::string(optarg));
            if (num_racks < 1) {
                panic("Number of racks should be > 0");
            }
            break;
        }
        case 'y': {
            if (strcmp(optarg, "fixed") == 0) {
                send_mode = memcachekv::SendMode::FIXED;
            } else if (strcmp(optarg, "dynamic") == 0) {
                send_mode = memcachekv::SendMode::DYNAMIC;
            } else {
                panic("Unknown send mode %s", optarg);
            }
            break;
        }
        case 'z': {
            num_nodes = stoi(std::string(optarg));
            if (num_nodes < 1) {
                panic("Number of nodes should be > 0");
            }
            break;
        }
        case 'A': {
            dec_interval = stoi(std::string(optarg));
            if (dec_interval < 1) {
                panic("Decrement interval should be > 0");
            }
            break;
        }
        case 'B': {
            n_dec = stoi(std::string(optarg));
            if (n_dec < 1) {
                panic("Decrement amount should be > 0");
            }
            break;
        }
        case 'C': {
            num_rkeys = stoi(std::string(optarg));
            if (num_rkeys < 0) {
                panic("num_rkeys should be >= 0");
            }
            break;
        }
        case 'D': {
            interval = stoi(std::string(optarg));
            if (interval < 0) {
                panic("interval should be >= 0");
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
                panic("Unknown dynamism type %s", optarg);
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
        case 'I': {
            app_core = stoi(std::string(optarg));
            break;
        }
        case 'J': {
            n_app_threads = stoi(std::string(optarg));
            break;
        }
        case 'K': {
            transport_core = stoi(std::string(optarg));
            break;
        }
        case 'L': {
            n_transport_threads = stoi(std::string(optarg));
            break;
        }
        case 'M': {
            colocate_id = stoi(std::string(optarg));
            break;
        }
        case 'N': {
            n_colocate_nodes = stoi(std::string(optarg));
            break;
        }
        default:
            panic("Unknown argument %s", argv[optind]);
        }
    }

    if (node_mode == NodeMode::UNKNOWN) {
        panic("Option -m <client/server> required");
    }

    if (app_mode == AppMode::UNKNOWN) {
        panic("Option -q required");
    }

    if (config_file_path == nullptr) {
        panic("Option -c <config file> required");
    }

    /* Initialize configuration and application */
    Configuration *config = nullptr;
    Application *app = nullptr;
    switch (transport_mode) {
    case TransportMode::UDP:
        config = new UDPConfiguration(config_file_path);
        break;
    case TransportMode::DPDK:
        config = new DPDKConfiguration(config_file_path);
        break;
    }
    config->duration = duration;
    config->num_racks = num_racks;
    config->num_nodes = num_nodes;
    config->transport_core = transport_core;
    config->n_transport_threads = n_transport_threads;
    config->app_core = app_core;
    config->n_app_threads = n_app_threads;
    config->colocate_id = colocate_id;
    config->n_colocate_nodes = n_colocate_nodes;
    config->use_endhost_lb = use_endhost_lb;

    Stats *stats = nullptr;
    memcachekv::KVWorkloadGenerator *gen = nullptr;
    memcachekv::MessageCodec *codec = nullptr;
    memcachekv::ControllerCodec *ctrl_codec = nullptr;

    switch (app_mode) {
    case AppMode::ECHO: {
        switch (node_mode) {
        case NodeMode::CLIENT: {
            if (node_id < 0) {
                panic("client requires argument '-e <node id>'");
            }
            config->rack_id = -1;
            config->client_id = node_id;
            config->node_type = Configuration::NodeType::CLIENT;
            config->terminating = true;
            config->use_raw_transport = false;
            stats = new Stats(n_app_threads, stats_file_path, 0);
            app = new echo::Client(config, stats, mean_interval);
            break;
        }
        case NodeMode::SERVER: {
            if (rack_id < 0) {
                panic("server requires argument '-r <rack id>'");
            }
            if (node_id < 0) {
                panic("server requires argument '-e <node id>'");
            }
            config->rack_id = rack_id;
            config->node_id = node_id;
            config->node_type = Configuration::NodeType::SERVER;
            config->terminating = false;
            config->use_raw_transport = false;
            app = new echo::Server();
            break;
        }
        default:
            panic("Unsupported node mode in application echo");
        }
        break;
    }
    case AppMode::MEMCACHEKV: {
        ctrl_codec = new memcachekv::ControllerCodec();

        switch (protocol_mode) {
        case ProtocolMode::STATIC:
            codec = new memcachekv::WireCodec(false);
            break;
        case ProtocolMode::PEGASUS:
            codec = new memcachekv::WireCodec(true);
            break;
        case ProtocolMode::NETCACHE:
            codec = new memcachekv::NetcacheCodec();
            break;
        default:
            panic("Unreachable");
        }

        switch (node_mode) {
        case NodeMode::CLIENT: {
            if (node_id < 0) {
                panic("client requires argument '-e <node id>'");
            }
            config->rack_id = -1;
            config->client_id = node_id;
            config->node_type = Configuration::NodeType::CLIENT;
            config->terminating = true;
            config->use_raw_transport = false;
            // Read in all keys
            std::ifstream in;
            in.open(keys_file_path);
            if (!in) {
                panic("Failed to read keys from %s", keys_file_path);
            }
            std::string key;
            for (int i = 0; i < nkeys; i++) {
                getline(in, key);
                keys.push_back(key);
            }
            in.close();

            stats = new memcachekv::MemcacheKVStats(n_app_threads,
                                                    stats_file_path,
                                                    interval,
                                                    interval_file_path);
            gen = new memcachekv::KVWorkloadGenerator(&keys,
                                                      value_len,
                                                      get_ratio,
                                                      put_ratio,
                                                      mean_interval,
                                                      target_latency,
                                                      alpha,
                                                      key_type,
                                                      send_mode,
                                                      d_type,
                                                      d_interval,
                                                      d_nkeys,
                                                      n_app_threads,
                                                      stats);

            app = new memcachekv::Client(config, (memcachekv::MemcacheKVStats*)stats, gen, codec);
            break;
        }
        case NodeMode::SERVER: {
            if (rack_id < 0) {
                panic("server requires argument '-r <rack id>'");
            }
            if (node_id < 0) {
                panic("server requires argument '-e <node id>'");
            }
            config->rack_id = rack_id;
            config->node_id = node_id;
            config->node_type = Configuration::NodeType::SERVER;
            config->terminating = false;
            config->use_raw_transport = false;
            std::string default_value = std::string(value_len, 'v');
            app = new memcachekv::Server(config, codec, ctrl_codec, proc_latency, default_value, report_load);
            break;
        }
        case NodeMode::CONTROLLER: {
            config->rack_id = -1;
            config->node_id = -1;
            config->client_id = -1;
            config->node_type = Configuration::NodeType::CLIENT;
            config->terminating = true;
            config->use_raw_transport = false;
            memcachekv::ControllerMessage msg;
            msg.type = memcachekv::ControllerMessage::Type::RESET_REQ;
            msg.reset_req.num_nodes = num_nodes;
            msg.reset_req.num_rkeys = num_rkeys;
            app = new memcachekv::Controller(config, msg);
            break;
        }
        case NodeMode::DECREMENTOR: {
            config->rack_id = -1;
            config->node_id = -1;
            config->client_id = -1;
            config->node_type = Configuration::NodeType::CLIENT;
            config->terminating = false;
            config->use_raw_transport = false;
            app = new memcachekv::Decrementor(config, dec_interval, n_dec);
            break;
        }
        case NodeMode::LB:
            config->rack_id = -1;
            config->node_id = -1;
            config->client_id = -1;
            config->node_type = Configuration::NodeType::LB;
            config->terminating = false;
            config->use_raw_transport = true;
            app = new memcachekv::LoadBalancer(config);
            break;
        default:
            panic("Unknown node mode");
        }
        break;
    }
    default:
        panic("Unknown application mode");
    }

    /* Initialize transport and node */
    Transport *transport = nullptr;
    switch (transport_mode) {
    case TransportMode::UDP:
        transport = new UDPTransport(config);
        break;
    case TransportMode::DPDK:
        transport = new DPDKTransport(config, use_flow_api);
        break;
    }
    Node *node = new Node(config, transport);

    /* Run application */
    node->register_app(app);
    node->run();

    /* Clean up */
    delete transport;
    delete config;
    delete node;
    delete app;
    delete ctrl_codec;
    delete codec;
    delete gen;
    delete stats;

    return 0;
}
