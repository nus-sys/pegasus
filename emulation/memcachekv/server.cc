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
        process_kv_request(msg.request, addr);
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        process_kv_migration(msg.migration_request, addr);
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
Server::process_kv_request(const MemcacheKVRequest &msg,
                           const sockaddr &addr)
{
    MemcacheKVMessage reply_msg;
    string reply_msg_str;
    reply_msg.type = MemcacheKVMessage::Type::REPLY;
    reply_msg.reply.client_id = msg.client_id;
    reply_msg.reply.req_id = msg.req_id;

    process_op(msg.op, reply_msg.reply);

    this->codec->encode(reply_msg_str, reply_msg);
    this->transport->send_message(reply_msg_str, addr);
}

void
Server::process_kv_migration(const MigrationRequest &msg,
                             const sockaddr &addr)
{
    if (insert_keyrange(msg.keyrange)) {
        for (const auto &op : msg.ops) {
            // Need to calculate keyhash: migration source server
            // does not calculate them
            keyhash_t keyhash = (keyhash_t)compute_keyhash(op.key);
            insert_kv(op.key, keyhash, op.value);
        }
    } else {
        // XXX Potentially a retransmission, should ack the switch again
        printf("kv migration request not processed. Range error\n");
        return;
    }
}

void
Server::process_op(const Operation &op, MemcacheKVReply &reply)
{
    if (!keyhash_in_range(op.keyhash)) {
        // XXX Buffer these requests until migration done
        printf("Key not in server's key range");
        return;
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
        panic("Unknown memcachekv op type");
    }
}

void
Server::process_ctrl_migration(const ControllerMigrationRequest &msg)
{
    MemcacheKVMessage kv_msg;
    std::string msg_str;
    kv_msg.type = MemcacheKVMessage::Type::MIGRATION_REQUEST;
    kv_msg.migration_request.keyrange = msg.keyrange;

    if (remove_keyrange(msg.keyrange)) {
        // Find the start of the key range
        // map.insert does not insert if element already exist
        auto res = this->key_hashes.insert(std::make_pair(msg.keyrange.start, std::set<std::string>()));
        auto it = res.first;
        if (res.second) {
            // keyhash 'start' does not exist
            it = this->key_hashes.erase(it);
        }
        while (it != this->key_hashes.end()) {
            if (it->first > msg.keyrange.end) {
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
    } else {
        // We have already migrated all the keys, this is a retransmit
        // from the controller.
        // XXX: Should resend the previous migration request to dst node
        // with all the kv pairs. For now just ignore.
        printf("KeyRange not in range");
        return;
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

    this->key_ranges.insert(msg.keyrange);
}

bool
Server::keyhash_in_range(keyhash_t keyhash)
{
    // Linear search (number of key ranges is small)
    for (const auto& keyrange : this->key_ranges) {
        if (keyhash < keyrange.start) {
            return false;
        }
        if (keyhash <= keyrange.end) {
            return true;
        }
    }
    return false;
}

bool
Server::insert_keyrange(const KeyRange &keyrange)
{
    // Search linearly and see if we can merge with any
    // existing key range
    KeyRange kr = keyrange;
    // To prevent overflow and underflow, cast to int64
    int64_t start = (int64_t)keyrange.start;
    int64_t end = (int64_t)keyrange.end;
    for (auto it = this->key_ranges.begin();
         it != this->key_ranges.end();
         ++it) {
        int64_t curr_start = (int64_t)it->start;
        int64_t curr_end = (int64_t)it->end;
        if (end < curr_start - 1) {
            // Install keyrange as a new key range
            break;
        }
        if (start > curr_end + 1) {
            continue;
        }
        if (start == curr_end + 1) {
            // Potential merge/double merge, look at the
            // next key range
            auto it_next = std::next(it);
            int64_t next_start = (int64_t)it_next->start;
            if (end >= next_start) {
                return false;
            }
            kr.start = it->start;
            this->key_ranges.erase(it);
            if (end == next_start - 1) {
                // Double merge
                kr.end = it_next->end;
                this->key_ranges.erase(it_next);
            } else {
                kr.end = keyrange.end;
            }
            break;
        }
        if (start >= curr_start ||
            end >= curr_start) {
            // Already exist
            return false;
        }
        if (end == curr_start - 1) {
            // Can safely merge, as we have done safety check and
            // double merge check in the previous iteration.
            kr.start = keyrange.start;
            kr.end = it->end;
            this->key_ranges.erase(it);
            break;
        }
    }
    this->key_ranges.insert(kr);
    return true;
}

bool
Server::remove_keyrange(const KeyRange &keyrange)
{
    // Linear search
    for (auto it = this->key_ranges.begin();
         it != this->key_ranges.end();
         ++it) {
        if (keyrange.start < it->start) {
            return false;
        }
        if (keyrange.end <= it->end) {
            KeyRange kr;
            if (keyrange.start == it->start) {
                kr.start = keyrange.end + 1;
                kr.end = it->end;
            } else if (keyrange.end == it->end) {
                kr.start = it->start;
                kr.end = keyrange.start - 1;
            } else {
                // XXX currently do not support
                // splitting keyrange in the middle
                panic("Splitting keyrange in the middle not supported!");
            }
            assert(kr.start <= kr.end);
            this->key_ranges.erase(it);
            this->key_ranges.insert(kr);
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
