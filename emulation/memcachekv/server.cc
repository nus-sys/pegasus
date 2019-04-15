#include <algorithm>
#include <functional>
#include <set>
#include "logger.h"
#include "utils.h"
#include "memcachekv/config.h"
#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

Server::Server(Configuration *config, MessageCodec *codec, ControllerCodec *ctrl_codec,
               int proc_latency, string default_value, bool report_load)
    : config(config),
    codec(codec),
    ctrl_codec(ctrl_codec),
    proc_latency(proc_latency),
    default_value(default_value),
    report_load(report_load)
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
    case ControllerMessage::Type::KEY_MGR: {
        process_ctrl_key_migration(msg.key_mgr);
        break;
    }
    default:
        panic("Server received unexpected controller message");
    }
}

void
Server::process_kv_request(const MemcacheKVRequest &request,
                           const sockaddr &addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    MemcacheKVMessage msg;
    string msg_str;

    process_op(request.op, msg.reply);

    // Chain replication: tail rack replies to client; other racks forward
    // request to the next rack (same node id) in chain
    if (this->config->rack_id == this->config->num_racks - 1) {
        msg.type = MemcacheKVMessage::Type::REPLY;
        msg.reply.node_id = this->config->node_id;
        msg.reply.client_id = request.client_id;
        msg.reply.req_id = request.req_id;

        this->codec->encode(msg_str, msg);
        if (request.client_addr.sa_family == 0) {
            // client_addr in request is empty: request is
            // directly sent from client -- send back to
            // src address
            this->transport->send_message(msg_str, addr);
        } else {
            // request has been forwarded along the chain:
            // send to original client address
            this->transport->send_message(msg_str, request.client_addr);
        }
    } else {
        msg.type = MemcacheKVMessage::Type::REQUEST;
        msg.request = request;
        if (this->config->rack_id == 0) {
            // we are the head rack: write client address into request
            // so that the tail rack can find the original client
            msg.request.client_addr = addr;
        }

        this->codec->encode(msg_str, msg);
        this->transport->send_message_to_node(msg_str,
                                              this->config->rack_id+1,
                                              this->config->node_id);
    }
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    reply.key = op.key;
    reply.keyhash = op.keyhash;
    reply.ver = op.ver;
    if (this->report_load) {
        reply.load = calculate_load();
    }
    switch (op.op_type) {
    case Operation::Type::GET: {
        reply.type = MemcacheKVReply::Type::READ;
        if (this->store.count(op.key) > 0) {
            // Key is present
            reply.result = Result::OK;
            reply.value = this->store.at(op.key).value;
        } else {
            // Key not found
            reply.result = Result::NOT_FOUND;
            reply.value = this->default_value;
        }
        break;
    }
    case Operation::Type::PUT: {
        reply.type = MemcacheKVReply::Type::WRITE;
        if (this->store.count(op.key) == 0 ||
            op.ver >= this->store.at(op.key).ver) {
            this->store[op.key] = Item(op.value, op.ver);
        }
        reply.result = Result::OK;
        reply.value = op.value; // for netcache
        break;
    }
    case Operation::Type::DEL: {
        reply.type = MemcacheKVReply::Type::WRITE;
        this->store.unsafe_erase(op.key);
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
Server::process_migration_request(const MigrationRequest &request)
{
    if (this->store.count(request.key) == 0 ||
        request.ver >= this->store.at(request.key).ver) {

        this->store[request.key] = Item(request.value, request.ver);

        MemcacheKVMessage msg;
        string msg_str;
        msg.type = MemcacheKVMessage::Type::MGR_ACK;
        msg.migration_ack.keyhash = request.keyhash;
        msg.migration_ack.ver = request.ver;
        msg.migration_ack.node_id = this->config->node_id;

        if (!this->codec->encode(msg_str, msg)) {
            printf("Failed to encode migration ack\n");
            return;
        }
        this->transport->send_message_to_router(msg_str);
    }
}

void
Server::process_ctrl_key_migration(const ControllerKeyMigration &key_mgr)
{
    MemcacheKVMessage msg;
    string msg_str;

    // Send migration request to all nodes in the rack (except itself)
    msg.type = MemcacheKVMessage::Type::MGR_REQ;
    msg.migration_request.keyhash = key_mgr.keyhash;
    msg.migration_request.key = key_mgr.key;
    if (this->store.count(key_mgr.key) == 0) {
        msg.migration_request.value = this->default_value;
        msg.migration_request.ver = 0;
    } else {
        msg.migration_request.value = this->store.at(key_mgr.key).value;
        msg.migration_request.ver = this->store.at(key_mgr.key).ver;
    }
    this->codec->encode(msg_str, msg);
    for (int node_id = 0; node_id < this->config->num_nodes; node_id++) {
        if (node_id != this->config->node_id) {
            this->transport->send_message_to_local_node(msg_str, node_id);
        }
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
