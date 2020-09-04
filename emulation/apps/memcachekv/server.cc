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
    : ver(BASE_VERSION), value("")
{
}

Server::Item::Item(ver_t ver, const std::string &value)
    : ver(ver), value(value)
{
}

Server::Item::Item(const Item &item)
    : ver(item.ver), value(item.value)
{
}

Server::Server(Configuration *config, MessageCodec *codec, ControllerCodec *ctrl_codec,
               int proc_latency, string default_value,
               std::deque<std::string> &keys)
    : config(config),
    codec(codec),
    ctrl_codec(ctrl_codec),
    proc_latency(proc_latency),
    default_value(default_value)
{
    for (const auto &key : keys) {
        this->store.insert(std::pair<std::string, Item>(key, Item(BASE_VERSION,
                                                                  default_value)));
    }
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
    // Do nothing
}

void Server::run_thread(int tid)
{
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
    switch (op.op_type) {
    case OpType::GET: {
        const_store_ac_t ac;
        if (this->store.find(ac, op.key)) {
            // Key is present
            reply.ver = ac->second.ver;
            reply.value = ac->second.value;
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
        {
            store_ac_t ac;
            this->store.insert(ac, op.key);
            if (op.ver >= ac->second.ver) {
                ac->second.ver = op.ver;
                ac->second.value = op.value;
            }
        }
        reply.ver = op.ver;
        reply.value = op.value; // for netcache
        reply.result = Result::OK;
        reply.op_type = OpType::PUT; // client doesn't expect PUTFWD
        break;
    }
    default:
        panic("Unknown memcachekv op type");
    }
}

void
Server::process_replication_request(const ReplicationRequest &request)
{
    bool reply = false;
    {
        store_ac_t ac;
        this->store.insert(ac, request.key);
        if (request.ver >= ac->second.ver) {
            ac->second.ver = request.ver;
            ac->second.value = request.value;
            reply = true;
        }
    }

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
    MemcacheKVMessage kvmsg;
    bool reply;
    {
        const_store_ac_t ac;
        if ((reply = this->store.find(ac, request.key))) {
            kvmsg.rc_request.ver = ac->second.ver;
            kvmsg.rc_request.value = ac->second.value;
        }
    }

    if (reply) {
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

} // namespace memcachekv
