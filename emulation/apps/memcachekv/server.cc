#include <algorithm>
#include <functional>
#include <set>
#include <unordered_map>

#include <logger.h>
#include <utils.h>
#include <apps/memcachekv/server.h>

#define BASE_VERSION 1

using std::string;

namespace memcachekv {

Server::Item::Item()
    : ver(0), value("")
{
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

Server::Item::Item(ver_t ver, const std::string &value)
    : ver(ver), value(value)
{
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

Server::Item::Item(const Item &item)
    : ver(item.ver), value(item.value)
{
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

Server::Server(Configuration *config, MessageCodec *codec, ControllerCodec *ctrl_codec,
               int proc_latency, string default_value, bool report_load,
               std::deque<std::string> &keys)
    : config(config),
    codec(codec),
    ctrl_codec(ctrl_codec),
    proc_latency(proc_latency),
    default_value(default_value),
    report_load(report_load)
{
    this->epoch_start.tv_sec = 0;
    this->epoch_start.tv_usec = 0;
    this->request_count.resize(config->n_transport_threads, 0);
    for (const auto &key : keys) {
        this->store.insert(std::pair<std::string, Item>(key, Item(BASE_VERSION,
                                                                  default_value)));
    }
    this->stats_lock = PTHREAD_RWLOCK_INITIALIZER;
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
        process_kv_message(kvmsg, addr, tid);
        return;
    }
    panic("Received unexpected message");
}

typedef std::function<bool(std::pair<keyhash_t, uint64_t>,
                           std::pair<keyhash_t, uint64_t>)> Comparator;
static Comparator comp =
[](std::pair<keyhash_t, uint64_t> a,
   std::pair<keyhash_t, uint64_t> b)
{
    return a.second > b.second;
};

void Server::run()
{
    //this->transport->run_app_threads(this);
}

void Server::run_thread(int tid)
{
    // Send HK report periodically
    while (true) {
        usleep(Server::STATS_EPOCH);
        /* Construct sorted hot key accesses */
        std::unordered_map<keyhash_t, count_t> hk;
        for (const auto &keyhash : this->hot_keys) {
            hk[keyhash] = this->access_count.at(keyhash);
        }
        std::set<std::pair<keyhash_t, count_t>, Comparator> sorted_hk(hk.begin(),
                                                                      hk.end(),
                                                                      comp);
        /* Clear stats */
        pthread_rwlock_wrlock(&this->stats_lock);
        this->access_count.clear();
        this->hot_keys.clear();
        pthread_rwlock_unlock(&this->stats_lock);
        /* Send hk report */
        if (sorted_hk.size() == 0) {
            continue;
        }
        // hk report has a max size of STATS_HK_REPORT_SIZE
        ControllerMessage ctrlmsg;
        ctrlmsg.type = ControllerMessage::Type::HK_REPORT;
        for (const auto &hk : sorted_hk) {
            if (ctrlmsg.hk_report.reports.size() >= Server::STATS_HK_REPORT_SIZE) {
                break;
            }
            ctrlmsg.hk_report.reports.push_back(ControllerHKReport::Report(hk.first,
                                                                           hk.second));
        }
        Message msg;
        if (!this->ctrl_codec->encode(msg, ctrlmsg)) {
            panic("Failed to encode hk report");
        }
        this->transport->send_message_to_controller(msg, this->config->rack_id);
    }
}

void Server::process_kv_message(const MemcacheKVMessage &msg,
                                const Address &addr, int tid)
{
    switch (msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        process_kv_request(msg.request, addr, tid);
        break;
    }
    case MemcacheKVMessage::Type::RC_REQ: {
        process_replication_request(msg.rc_request);
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
    case ControllerMessage::Type::REPLICATION: {
        process_ctrl_replication(msg.replication);
        break;
    }
    default:
        panic("Server received unexpected controller message");
    }
}

void Server::process_kv_request(const MemcacheKVRequest &request,
                                const Address &addr,
                                int tid)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    MemcacheKVMessage kvmsg;
    process_op(request.op, kvmsg.reply, tid);

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
Server::process_op(const Operation &op, MemcacheKVReply &reply, int tid)
{
    reply.op_type = op.op_type;
    reply.keyhash = op.keyhash;
    reply.key = op.key;
    if (this->report_load) {
        reply.load = calculate_load();
    }
    switch (op.op_type) {
    case OpType::GET: {
        auto it = this->store.find(op.key);
        if (it != this->store.end()) {
            // Key is present
            pthread_rwlock_rdlock(&it->second.lock);
            reply.ver = it->second.ver;
            reply.value = it->second.value;
            pthread_rwlock_unlock(&it->second.lock);
            reply.result = Result::OK;
        } else {
            // Key not found
            reply.ver = BASE_VERSION;
            reply.value = std::string("");
            reply.result = Result::NOT_FOUND;
        }
        break;
    }
    case OpType::PUT:
    case OpType::PUTFWD: {
        Item &item = this->store[op.key];
        pthread_rwlock_wrlock(&item.lock);
        if (op.ver >= item.ver) {
            item.ver = op.ver;
            item.value = op.value;
        }
        pthread_rwlock_unlock(&item.lock);
        reply.ver = op.ver;
        reply.value = op.value; // for netcache
        reply.result = Result::OK;
        reply.op_type = OpType::PUT; // client doesn't expect PUTFWD
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
    //update_rate(op, tid);
}

void
Server::process_replication_request(const ReplicationRequest &request)
{
    bool reply = false;
    Item &item = this->store[request.key];
    pthread_rwlock_wrlock(&item.lock);
    if (request.ver >= item.ver) {
        item.value = request.value;
        item.ver = request.ver;
        reply = true;
    }
    pthread_rwlock_unlock(&item.lock);

    if (reply) {
        MemcacheKVMessage kvmsg;
        kvmsg.type = MemcacheKVMessage::Type::RC_ACK;
        kvmsg.rc_ack.keyhash = request.keyhash;
        kvmsg.rc_ack.ver = request.ver;
        kvmsg.rc_ack.server_id = this->config->node_id;

        Message msg;
        if (!this->codec->encode(msg, kvmsg)) {
            panic("Failed to encode migration ack");
        }
        this->transport->send_message_to_lb(msg);
    }
}

void
Server::process_ctrl_replication(const ControllerReplication &request)
{
    auto it = this->store.find(request.key);
    if (it != this->store.end()) {
        MemcacheKVMessage kvmsg;

        pthread_rwlock_rdlock(&it->second.lock);
        kvmsg.rc_request.value = it->second.value;
        kvmsg.rc_request.ver = it->second.ver;
        pthread_rwlock_unlock(&it->second.lock);

        kvmsg.type = MemcacheKVMessage::Type::RC_REQ;
        kvmsg.rc_request.keyhash = request.keyhash;
        kvmsg.rc_request.key = request.key;

        // Send replication request to all nodes in the rack (except itself)
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
}

void
Server::update_rate(const Operation &op, int tid)
{
    if (++this->request_count[tid] % Server::STATS_SAMPLE_RATE == 0) {
        pthread_rwlock_rdlock(&this->stats_lock);
        if (++this->access_count[op.keyhash] >= Server::STATS_HK_THRESHOLD) {
            this->hot_keys.insert(op.keyhash);
        }
        pthread_rwlock_unlock(&this->stats_lock);
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
