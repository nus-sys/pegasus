#include <algorithm>
#include <functional>
#include <set>

#include <logger.h>
#include <utils.h>
#include <apps/memcachekv/server.h>

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

Server::~Server()
{
}

void Server::receive_message(const Message &msg, const Address &addr, int tid)
{
    // Check for controller message
    ControllerMessage ctrlmsg;
    if (this->ctrl_codec->decode(msg, ctrlmsg)) {
        process_ctrl_message(ctrlmsg, addr);
        return;
    }

    // KV message
    MemcacheKVMessage kvmsg;
    if (this->codec->decode(msg, kvmsg)) {
        process_kv_message(kvmsg, addr);
        return;
    }
    panic("Received unexpected message");
}

typedef std::function<bool(std::pair<keyhash_t, unsigned int>,
                           std::pair<keyhash_t, unsigned int>)> Comparator;
static Comparator comp =
[](std::pair<keyhash_t, unsigned int> a,
   std::pair<keyhash_t, unsigned int> b)
{
    return a.second > b.second;
};

void Server::run()
{
    this->transport->run_app_threads(this);
}

void Server::run_thread(int tid)
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
        ControllerMessage ctrlmsg;
        ctrlmsg.type = ControllerMessage::Type::HK_REPORT;
        int i = 0;
        for (const auto &hk : sorted_hk) {
            if (i >= MAX_HK_SIZE) {
                break;
            }
            ctrlmsg.hk_report.reports.push_back(ControllerHKReport::Report(hk.first, hk.second));
            i++;
        }
        Message msg;
        if (!this->ctrl_codec->encode(msg, ctrlmsg)) {
            panic("Failed to encode hk report");
        }
        this->transport->send_message_to_controller(msg, this->config->rack_id);
    }
}

void Server::process_kv_message(const MemcacheKVMessage &msg,
                                const Address &addr)
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

void Server::process_ctrl_message(const ControllerMessage &msg,
                                  const Address &addr)
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

void Server::process_kv_request(const MemcacheKVRequest &request,
                                const Address &addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    MemcacheKVMessage kvmsg;
    process_op(request.op, kvmsg.reply);

    // Chain replication: tail rack sends a reply; other racks forward the request
    if (this->config->rack_id == this->config->num_racks - 1) {
        kvmsg.type = MemcacheKVMessage::Type::REPLY;
        kvmsg.reply.client_id = request.client_id;
        kvmsg.reply.server_id = this->config->node_id;
        kvmsg.reply.req_id = request.req_id;
        kvmsg.reply.req_time = request.req_time;
    } else {
        kvmsg.type = MemcacheKVMessage::Type::REQUEST;
        kvmsg.request = request;
        kvmsg.request.op.op_type = OpType::PUTFWD;
    }
    Message msg;
    if (!this->codec->encode(msg, kvmsg)) {
        panic("Failed to encode message");
    }

    if (this->config->use_endhost_lb) {
        this->transport->send_message_to_lb(msg);
    } else {
        // Chain replication: tail rack replies to client; other racks forward
        // request to the next rack (same node id) in chain
        if (this->config->rack_id == this->config->num_racks - 1) {
            this->transport->send_message(msg,
                    *this->config->client_addresses[request.client_id]);
        } else {
            this->transport->send_message_to_node(msg,
                    this->config->rack_id+1,
                    this->config->node_id);
        }
    }
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    reply.op_type = op.op_type;
    reply.keyhash = op.keyhash;
    reply.ver = op.ver;
    reply.key = op.key;
    if (this->report_load) {
        reply.load = calculate_load();
    }
    switch (op.op_type) {
    case OpType::GET: {
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
    case OpType::PUT:
    case OpType::PUTFWD: {
        if (this->store.count(op.key) == 0 ||
            op.ver >= this->store.at(op.key).ver) {
            this->store[op.key].value = op.value;
            this->store[op.key].ver = op.ver;
        }
        reply.op_type = OpType::PUT; // client doesn't expect PUTFWD
        reply.result = Result::OK;
        reply.value = op.value; // for netcache
        break;
    }
    case OpType::DEL: {
        this->store.unsafe_erase(op.key);
        // XXX rkey?
        reply.result = Result::OK;
        reply.value = "";
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
    //update_rate(op);
}

void
Server::process_migration_request(const MigrationRequest &request)
{
    if (this->store.count(request.key) == 0 ||
        request.ver >= this->store.at(request.key).ver) {

        this->store[request.key] = Item(request.value, request.ver);

        MemcacheKVMessage kvmsg;
        kvmsg.type = MemcacheKVMessage::Type::MGR_ACK;
        kvmsg.migration_ack.keyhash = request.keyhash;
        kvmsg.migration_ack.ver = request.ver;
        kvmsg.migration_ack.server_id = this->config->node_id;

        Message msg;
        if (!this->codec->encode(msg, kvmsg)) {
            panic("Failed to encode migration ack");
        }
        this->transport->send_message_to_lb(msg);
    }
}

void
Server::process_ctrl_key_migration(const ControllerKeyMigration &key_mgr)
{
    MemcacheKVMessage kvmsg;

    // Send migration request to all nodes in the rack (except itself)
    kvmsg.type = MemcacheKVMessage::Type::MGR_REQ;
    kvmsg.migration_request.keyhash = key_mgr.keyhash;
    kvmsg.migration_request.key = key_mgr.key;
    if (this->store.count(key_mgr.key) == 0) {
        kvmsg.migration_request.value = this->default_value;
        kvmsg.migration_request.ver = 0;
    } else {
        kvmsg.migration_request.value = this->store.at(key_mgr.key).value;
        kvmsg.migration_request.ver = this->store.at(key_mgr.key).ver;
    }
    Message msg;
    if (!this->codec->encode(msg, kvmsg)) {
        panic("Failed to encode message");
    }
    for (int node_id = 0; node_id < this->config->num_nodes; node_id++) {
        if (node_id != this->config->node_id) {
            this->transport->send_message_to_local_node(msg, node_id);
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
    this->load_mutex.lock();
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
    this->load_mutex.unlock();

    return this->request_ts.size();
}

} // namespace memcachekv
