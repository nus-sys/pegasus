#include "logger.h"
#include "utils.h"
#include "memcachekv/config.h"
#include "memcachekv/server.h"

using std::string;

namespace memcachekv {

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
    this->codec->decode(message, kv_msg);
    process_kv_message(kv_msg, src_addr);
}

void
Server::run(int duration)
{
    // Register with the controller
    ControllerMessage msg;
    string msg_str;
    msg.type = ControllerMessage::Type::REGISTER_REQ;
    msg.reg_req.node_id = this->node_id;
    this->ctrl_codec->encode(msg_str, msg);
    this->transport->send_message_to_controller(msg_str);
}

void
Server::process_kv_message(const MemcacheKVMessage &msg,
                           const sockaddr &addr)
{
    // User defined processing latency
    if (this->proc_latency > 0) {
        wait(this->proc_latency);
    }

    switch (msg.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        MemcacheKVMessage reply_msg;
        string reply_msg_str;
        reply_msg.type = MemcacheKVMessage::Type::REPLY;
        reply_msg.reply.client_id = msg.request.client_id;
        reply_msg.reply.req_id = msg.request.req_id;

        process_op(msg.request.op, reply_msg.reply);

        this->codec->encode(reply_msg_str, reply_msg);
        this->transport->send_message(reply_msg_str, addr);
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        for (const auto &op : msg.migration_request.ops) {
            // Need to calculate keyhash: migration source server
            // does not calculate them
            keyhash_t keyhash = (keyhash_t)compute_keyhash(op.key);
            insert_kv(op.key, keyhash, op.value);
        }
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
    case ControllerMessage::Type::MIGRATION_REQ: {
        process_ctrl_migration(msg.migration_req);
        break;
    }
    case ControllerMessage::Type::REGISTER_REPLY: {
        process_ctrl_register(msg.reg_reply);
        break;
    }
    default:
        panic("Server received unexpected controller message");
    }
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    if (!keyhash_in_range(op.keyhash)) {
        // XXX Buffer these requests until migration done
        panic("Key not in server's key range");
    }

    switch (op.op_type) {
    case Operation::Type::GET: {
        if (this->store.count(op.key) > 0) {
            // Key is present
            reply.result = Result::OK;
            reply.value = this->store.at(op.key);
        } else {
            // Key not found
            reply.result = Result::NOT_FOUND;
        }
        break;
    }
    case Operation::Type::PUT: {
        insert_kv(op.key, op.keyhash, op.value);
        reply.result = Result::OK;
        break;
    }
    case Operation::Type::DEL: {
        remove_kv(op.key, op.keyhash);
        reply.result = Result::OK;
        break;
    }
    default:
        printf("Unknown memcachekv op type %d\n", op.op_type);
    }
}

void
Server::process_ctrl_migration(const ControllerMigrationRequest &msg)
{
    MemcacheKVMessage kv_msg;
    std::string msg_str;
    kv_msg.type = MemcacheKVMessage::Type::MIGRATION_REQUEST;
    // Find the start of the key range
    // map.insert does not insert if element already present
    auto res = this->key_hashes.insert(std::make_pair(msg.key_range.start, std::set<std::string>()));
    auto it = res.first;
    if (res.second) {
        // keyhash 'start' does not exist
        it = this->key_hashes.erase(it);
    }
    while (it != this->key_hashes.end()) {
        if (it->first > msg.key_range.end) {
            break;
        }

        for (const auto& key : it->second) {
            kv_msg.migration_request.ops.push_back(Operation());
            Operation &op = kv_msg.migration_request.ops.back();
            op.op_type = Operation::Type::PUT;
            op.key = key;
            op.value = this->store.at(key);
            this->store.erase(key);
        }
        it = this->key_hashes.erase(it);
    }

    this->codec->encode(msg_str, kv_msg);
    this->transport->send_message_to_node(msg_str, msg.dst_node_id);
}

void
Server::process_ctrl_register(const ControllerRegisterReply &msg)
{
    if (this->key_ranges.size() > 0) {
        // Already registered
        return;
    }

    this->key_ranges.insert(msg.key_range);
}

bool
Server::keyhash_in_range(keyhash_t keyhash)
{
    // Linear search (number of key ranges is small)
    for (const auto& key_range : this->key_ranges) {
        if (keyhash >= key_range.start && keyhash <= key_range.end) {
            return true;
        }
    }
    return false;
}

void
Server::insert_kv(const string &key, keyhash_t keyhash, const string &value)
{
    this->key_hashes[keyhash].insert(key);
    this->store[key] = value;
}

void
Server::remove_kv(const string &key, keyhash_t keyhash)
{
    if (this->key_hashes.count(keyhash) > 0) {
        this->key_hashes[keyhash].erase(key);
        if (this->key_hashes[keyhash].size() == 0) {
            this->key_hashes.erase(keyhash);
        }
    }
    this->store.erase(key);
}

} // namespace memcachekv
