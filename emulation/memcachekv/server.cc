#include <functional>
#include <set>
#include "logger.h"
#include "utils.h"
#include "memcachekv/config.h"
#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

Server::Server(Transport *transport, Configuration *config, MessageCodec *codec,
       ControllerCodec *ctrl_codec, int node_id, int proc_latency)
    : transport(transport),
    config(config),
    codec(codec),
    ctrl_codec(ctrl_codec),
    node_id(node_id),
    proc_latency(proc_latency)
{
    this->epoch_start.tv_sec = 0;
    this->epoch_start.tv_usec = 0;
    this->request_count = 0;
}

void
Server::receive_message(const string &message, const sockaddr &src_addr)
{
    // Check for controller message
    ControllerMessage ctrl_msg;
    if (this->ctrl_codec->decode(message, ctrl_msg)) {
        process_ctrl_message(ctrl_msg, src_addr);
        return;
    }

    // KV message
    MemcacheKVMessage kv_msg;
    if (this->codec->decode(message, kv_msg)) {
        process_kv_message(kv_msg, src_addr);
        return;
    }
}

typedef std::function<bool(std::pair<keyhash_t, unsigned int>,
                           std::pair<keyhash_t, unsigned int>)> Comparator;
static Comparator comp =
[](std::pair<keyhash_t, unsigned int> a,
   std::pair<keyhash_t, unsigned int> b)
{
    return a.second > b.second;
};

void
Server::run(int duration)
{
    // Send HK report periodically
    while (true) {
        usleep(HK_EPOCH);
        // Sort hk_report
        this->hk_mutex.lock();
        std::set<std::pair<keyhash_t, unsigned int>, Comparator> sorted_hk(this->hk_report.begin(), this->hk_report.end(), comp);
        this->hk_report.clear();
        this->key_count.clear();
        this->hk_mutex.unlock();

        if (sorted_hk.size() == 0) {
            continue;
        }

        // hk report has a max of MAX_HK_SIZE reports
        ControllerMessage msg;
        string msg_str;
        msg.type = ControllerMessage::Type::HK_REPORT;
        int i = 0;
        for (const auto &hk : sorted_hk) {
            if (i >= MAX_HK_SIZE) {
                break;
            }
            msg.hk_report.reports.push_back(ControllerHKReport::Report(hk.first, hk.second));
            i++;
        }
        if (this->ctrl_codec->encode(msg_str, msg)) {
            this->transport->send_message_to_controller(msg_str);
        } else {
            printf("Failed to encode hk report\n");
        }
    }
}

void
Server::process_kv_message(const MemcacheKVMessage &msg,
                           const sockaddr &addr)
{
    switch (msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        process_kv_request(msg.request, addr);
        break;
    }
    case MemcacheKVMessage::Type::MGR_REQ: {
        process_migration_request(msg.migration_request);
        break;
    }
    default:
        panic("Server received unexpected kv message");
    }
}

void
Server::process_ctrl_message(const ControllerMessage &msg,
                             const sockaddr &addr)
{
    switch (msg.type) {
    default:
        panic("Server received unexpected controller message");
    }
}

void
Server::process_kv_request(const MemcacheKVRequest &msg,
                           const sockaddr &addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    MemcacheKVMessage reply_msg;
    string reply_msg_str;
    reply_msg.type = MemcacheKVMessage::Type::REPLY;
    reply_msg.reply.node_id = this->node_id;
    reply_msg.reply.client_id = msg.client_id;
    reply_msg.reply.req_id = msg.req_id;

    process_op(msg.op, reply_msg.reply);

    this->codec->encode(reply_msg_str, reply_msg);
    this->transport->send_message(reply_msg_str, addr);
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    //printf("Received request type %u keyhash %u ver %u key %s\n", op.op_type, op.keyhash, op.ver, op.key.c_str());

    reply.keyhash = op.keyhash;
    reply.ver = op.ver;
    switch (op.op_type) {
    case Operation::Type::GET:
    case Operation::Type::MGR: {
        reply.type = MemcacheKVReply::Type::READ;
        if (this->store.count(op.key) > 0) {
            // Key is present
            reply.result = Result::OK;
            reply.value = this->store.at(op.key).value;
        } else {
            // Key not found
            reply.result = Result::NOT_FOUND;
            reply.value = "";
        }
        if (op.op_type == Operation::Type::MGR) {
            process_migration(op, reply.value, op.node_id);
        }
        break;
    }
    case Operation::Type::PUT: {
        reply.type = MemcacheKVReply::Type::WRITE;
        if (op.ver >= this->store[op.key].ver) {
            this->store[op.key].value = op.value;
            if (op.ver > this->store[op.key].ver) {
                // Rkey has a new version, can clear the replica set
                this->store[op.key].ver = op.ver;
                this->replicated_keys[op.key].replicas.clear();
                if (op.num_replicas > 1) {
                    // replicate to local replicas
                    // XXX currently send to everyone
                    for (int i = 0; i < this->config->num_nodes; i++) {
                        if (i != this->node_id) {
                            process_migration(op, op.value, i);
                        }
                    }
                }
            }
        }
        reply.result = Result::OK;
        reply.value = "";
        break;
    }
    case Operation::Type::DEL: {
        reply.type = MemcacheKVReply::Type::WRITE;
        this->store.erase(op.key);
        // XXX rkey?
        reply.result = Result::OK;
        reply.value = "";
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
    update_rate(op);
}

void
Server::process_migration(const Operation &op,
                          const string &value,
                          int dst)
{
    // Only send migration request if:
    // 1. not to itself
    // 2. has not sent to the targeted node before
    if (dst != this->node_id) {
        if (this->replicated_keys[op.key].replicas.count(dst) == 0) {
            this->replicated_keys[op.key].replicas.insert(dst);

            MemcacheKVMessage mgr_req;
            string mgr_req_str;
            mgr_req.type = MemcacheKVMessage::Type::MGR_REQ;
            mgr_req.migration_request.keyhash = op.keyhash;
            mgr_req.migration_request.ver = op.ver;
            mgr_req.migration_request.key = op.key;
            mgr_req.migration_request.value = value;
            this->codec->encode(mgr_req_str, mgr_req);
            this->transport->send_message_to_node(mgr_req_str, dst);
        }
    }
}

void
Server::process_migration_request(const MigrationRequest &request)
{
    //printf("Received migration keyhash %u ver %u key %s value %s\n", request.keyhash, request.ver, request.key.c_str(), request.value.c_str());

    if (this->store.count(request.key) == 0 ||
        request.ver > this->store[request.key].ver) {

        this->store[request.key].value = request.value;
        this->store[request.key].ver = request.ver;
        this->replicated_keys[request.key].replicas.clear();

        MemcacheKVMessage msg;
        string msg_str;
        msg.type = MemcacheKVMessage::Type::MGR_ACK;
        msg.migration_ack.keyhash = request.keyhash;
        msg.migration_ack.ver = request.ver;
        msg.migration_ack.node_id = this->node_id;

        if (!this->codec->encode(msg_str, msg)) {
            printf("Failed to encode migration ack\n");
            return;
        }
        this->transport->send_message_to_addr(msg_str, this->config->router_address);
    }
}

void
Server::update_rate(const Operation &op)
{
    if (++this->request_count % KR_SAMPLE_RATE == 0) {
        this->hk_mutex.lock();
        if (++this->key_count[op.keyhash] >= this->HK_THRESHOLD) {
            this->hk_report[op.keyhash] = this->key_count[op.keyhash];
        }
        this->hk_mutex.unlock();
    }
}

load_t
Server::calculate_load()
{
    struct timeval now;

    gettimeofday(&now, nullptr);
    if (this->epoch_start.tv_sec == 0 && this->epoch_start.tv_usec == 0) {
        // Initialize epoch
        this->epoch_start = now;
    }
    this->request_ts.push_back(now);

    if (latency(this->epoch_start, now) > EPOCH_DURATION) {
        this->epoch_start = get_prev_timeval(now, EPOCH_DURATION);
        for (auto it = this->request_ts.begin(); it != this->request_ts.end(); ) {
            // Remove ts that fall out of the epoch
            if (timeval_cmp(*it, this->epoch_start) < 0) {
                it = this->request_ts.erase(it);
            } else {
                // ts should be in order
                break;
            }
        }
    }

    return this->request_ts.size();
}

} // namespace memcachekv
