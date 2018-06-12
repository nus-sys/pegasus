#include "logger.h"
#include "memcachekv/message.h"
#include "memcachekv/config.h"

using std::string;

namespace memcachekv {

void convert_endian(void *dst, void *src, size_t size)
{
    uint8_t *dptr, *sptr;
    for (dptr = (uint8_t*)dst, sptr = (uint8_t*)src + size - 1;
         size > 0;
         size--) {
        *dptr++ = *sptr--;
    }
}

bool
ProtobufCodec::decode(const string &in, MemcacheKVMessage &out)
{
    proto::MemcacheKVMessage msg;
    msg.ParseFromString(in);

    if (msg.has_request()) {
        out.type = MemcacheKVMessage::Type::REQUEST;
        out.request = MemcacheKVRequest(msg.request());
    } else if (msg.has_reply()) {
        out.type = MemcacheKVMessage::Type::REPLY;
        out.reply = MemcacheKVReply(msg.reply());
    } else {
        panic("protobuf MemcacheKVMessage wrong format");
    }
    return true;
}

bool
ProtobufCodec::encode(string &out, const MemcacheKVMessage &in)
{
    proto::MemcacheKVMessage msg;

    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        msg.mutable_request()->set_client_id(in.request.client_id);
        msg.mutable_request()->set_req_id(in.request.req_id);
        proto::Operation op;
        op.set_op_type(static_cast<proto::Operation_Type>(in.request.op.op_type));
        op.set_key(in.request.op.key);
        op.set_value(in.request.op.value);
        *(msg.mutable_request()->mutable_op()) = op;
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        msg.mutable_reply()->set_client_id(in.reply.client_id);
        msg.mutable_reply()->set_req_id(in.reply.req_id);
        msg.mutable_reply()->set_result(static_cast<proto::Result>(in.reply.result));
        msg.mutable_reply()->set_value(in.reply.value);
        break;
    }
    default:
        panic("MemcacheKVMessage wrong format");
    }

    msg.SerializeToString(&out);
    return true;
}

bool
WireCodec::decode(const std::string &in, MemcacheKVMessage &out)
{
    const char *buf = in.data();
    const char *ptr = buf;
    size_t buf_size = in.size();

    if (buf_size < PACKET_BASE_SIZE) {
        return false;
    }
    if (*(identifier_t*)ptr != PEGASUS) {
        return false;
    }
    ptr += sizeof(identifier_t);
    op_type_t op_type = *(op_type_t*)ptr;
    ptr += sizeof(op_type_t);
    keyhash_t keyhash;
    convert_endian(&keyhash, ptr, sizeof(keyhash_t));
    ptr += sizeof(keyhash_t);

    switch (op_type) {
    case OP_GET:
    case OP_PUT:
    case OP_DEL: {
        if (buf_size < REQUEST_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REQUEST;
        out.request.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.request.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        if (op_type == OP_GET) {
            out.request.op.op_type = Operation::Type::GET;
        } else if (op_type == OP_PUT) {
            out.request.op.op_type = Operation::Type::PUT;
        } else {
            out.request.op.op_type = Operation::Type::DEL;
        }
        ptr += sizeof(op_type_t);
        key_len_t key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        if (buf_size < REQUEST_BASE_SIZE + key_len) {
            return false;
        }
        out.request.op.key = string(ptr, key_len);
        ptr += key_len;
        out.request.op.keyhash = keyhash;
        if (op_type == OP_PUT) {
            if (buf_size < REQUEST_BASE_SIZE + key_len + sizeof(value_len_t)) {
                return false;
            }
            value_len_t value_len = *(value_len_t*)ptr;
            ptr += sizeof(value_len_t);
            if (buf_size < REQUEST_BASE_SIZE + key_len + sizeof(value_len_t) + value_len) {
                return false;
            }
            out.request.op.value = string(ptr, value_len);
        }
        break;
    }
    case OP_REP: {
        if (buf_size < REPLY_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REPLY;
        out.reply.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.reply.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.reply.result = static_cast<Result>(*(result_t*)ptr);
        ptr += sizeof(result_t);
        value_len_t value_len = *(value_len_t*)ptr;
        ptr += sizeof(value_len_t);
        if (buf_size < REPLY_BASE_SIZE + value_len) {
            return false;
        }
        out.reply.value = string(ptr, value_len);
        break;
    }
    case OP_MGR: {
        assert(buf_size > MIGRATION_REQUEST_BASE_SIZE);
        out.type = MemcacheKVMessage::Type::MIGRATION_REQUEST;
        out.migration_request.keyrange.start = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        out.migration_request.keyrange.end = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        nops_t nops = *(nops_t *)ptr;
        ptr += sizeof(nops_t);
        if (out.migration_request.ops.capacity() < nops) {
            out.migration_request.ops.reserve(nops);
        }
        for (nops_t i = 0; i < nops; i++) {
            out.migration_request.ops.push_back(Operation());
            Operation &op = out.migration_request.ops.back();
            op.op_type = Operation::Type::PUT;
            key_len_t key_len = *(key_len_t *)ptr;
            ptr += sizeof(key_len_t);
            op.key = string(ptr, key_len);
            ptr += key_len + 1;
            value_len_t value_len = *(value_len_t *)ptr;
            ptr += sizeof(value_len_t);
            op.value = string(ptr, value_len);
            ptr += value_len + 1;
        }
        break;
    }
    default:
        panic("Unknown packet type");
    }
}

void
WireCodec::encode(std::string &out, const MemcacheKVMessage &in)
{
    // First determine buffer size
    size_t buf_size;
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        // +1 for the terminating null
        buf_size = REQUEST_BASE_SIZE + in.request.op.key.size() + 1;
        if (in.request.op.op_type == Operation::Type::PUT) {
            buf_size += sizeof(value_len_t) + in.request.op.value.size() + 1;
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        buf_size = REPLY_BASE_SIZE + in.reply.value.size() + 1;
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        buf_size = MIGRATION_REQUEST_BASE_SIZE;
        for (const auto &op : in.migration_request.ops) {
            buf_size += sizeof(key_len_t) + op.key.size() + 1 + sizeof(value_len_t) + op.value.size() + 1;
        }
        break;
    }
    default:
        panic("Input message wrong format");
    }

    char *buf = new char[buf_size];
    char *ptr = buf;
    // App header
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        *(type_t *)ptr = TYPE_REQUEST;
        ptr += sizeof(type_t);
        *(rsvd_t *)ptr = 0;
        ptr += sizeof(rsvd_t);
        *(port_t *)ptr = 0;
        ptr += sizeof(port_t);
        uint64_t hash = compute_keyhash(in.request.op.key);
        const uint8_t *hash_ptr = (uint8_t *)&hash;
        // Big Endian
        for (size_t i = 0; i < sizeof(keyhash_t); i++) {
            *(uint8_t *)(ptr + sizeof(keyhash_t) - i - 1) = *hash_ptr++;
        }
        ptr += sizeof(keyhash_t);
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        *(type_t *)ptr = TYPE_REPLY;
        // No keyhash required
        ptr += PACKET_BASE_SIZE;
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        *(type_t *)ptr = TYPE_MIGRATION_REQUEST;
        // No keyhash required
        ptr += PACKET_BASE_SIZE;
        break;
    }
    default:
        panic("Input message wrong format");
    }

    // Payload
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        *(client_id_t *)ptr = (client_id_t)in.request.client_id;
        ptr += sizeof(client_id_t);
        *(req_id_t *)ptr = (req_id_t)in.request.req_id;
        ptr += sizeof(req_id_t);
        *(op_type_t *)ptr = (op_type_t)in.request.op.op_type;
        ptr += sizeof(op_type_t);
        *(key_len_t *)ptr = (key_len_t)in.request.op.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.request.op.key.data(), in.request.op.key.size());
        ptr += in.request.op.key.size();
        *(ptr++) = '\0';
        if (in.request.op.op_type == Operation::Type::PUT) {
            *(value_len_t *)ptr = (value_len_t)in.request.op.value.size();
            ptr += sizeof(value_len_t);
            memcpy(ptr, in.request.op.value.data(), in.request.op.value.size());
            ptr += in.request.op.value.size();
            *(ptr++) = '\0';
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        *(client_id_t *)ptr = (client_id_t)in.reply.client_id;
        ptr += sizeof(client_id_t);
        *(req_id_t *)ptr = (req_id_t)in.reply.req_id;
        ptr += sizeof(req_id_t);
        *(result_t *)ptr = (result_t)in.reply.result;
        ptr += sizeof(result_t);
        *(value_len_t *)ptr = (value_len_t)in.reply.value.size();
        ptr += sizeof(value_len_t);
        memcpy(ptr, in.reply.value.data(), in.reply.value.size());
        ptr += in.reply.value.size();
        *(ptr++) = '\0';
        break;
    }
    case MemcacheKVMessage::Type::MIGRATION_REQUEST: {
        *(keyhash_t *)ptr = (keyhash_t)in.migration_request.keyrange.start;
        ptr += sizeof(keyhash_t);
        *(keyhash_t *)ptr = (keyhash_t)in.migration_request.keyrange.end;
        ptr += sizeof(keyhash_t);
        *(nops_t *)ptr = (nops_t)in.migration_request.ops.size();
        ptr += sizeof(nops_t);
        for (const auto &op : in.migration_request.ops) {
            *(key_len_t *)ptr = (key_len_t)op.key.size();
            ptr += sizeof(key_len_t);
            memcpy(ptr, op.key.data(), op.key.size());
            ptr += op.key.size();
            *(ptr++) = '\0';
            *(value_len_t *)ptr = (value_len_t)op.value.size();
            ptr += sizeof(value_len_t);
            memcpy(ptr, op.value.data(), op.value.size());
            ptr += op.value.size();
            *(ptr++) = '\0';
        }
        break;
    }
    default:
        panic("Input message wrong format");
    }

    out = string(buf, buf_size);
    delete[] buf;
}

bool
ControllerCodec::encode(std::string &out, const ControllerMessage &in)
{
    size_t buf_size;
    switch (in.type) {
    case ControllerMessage::Type::RESET_REQ: {
        buf_size = RESET_REQ_SIZE;
        break;
    }
    case ControllerMessage::Type::RESET_REPLY: {
        buf_size = RESET_REPLY_SIZE;
        break;
    }
    case ControllerMessage::Type::MIGRATION_REQ: {
        buf_size = MIGRATION_REQ_SIZE;
        break;
    }
    case ControllerMessage::Type::MIGRATION_REPLY: {
        buf_size = MIGRATION_REPLY_SIZE;
        break;
    }
    case ControllerMessage::Type::REGISTER_REQ: {
        buf_size = REGISTER_REQ_SIZE;
        break;
    }
    case ControllerMessage::Type::REGISTER_REPLY: {
        buf_size = REGISTER_REPLY_SIZE;
        break;
    }
    default:
        panic("Unexpected controller message type");
    }

    char *buf = new char[buf_size];
    char *ptr = buf;
    *(identifier_t*)ptr = IDENTIFIER;
    ptr += sizeof(identifier_t);
    *(type_t*)ptr = static_cast<type_t>(in.type);
    ptr += sizeof(type_t);

    switch (in.type) {
    case ControllerMessage::Type::RESET_REQ: {
        *(num_nodes_t*)ptr = in.reset_req.num_nodes;
        ptr += sizeof(num_nodes_t);
        *(lb_type_t*)ptr = static_cast<lb_type_t>(in.reset_req.lb_type);
        break;
    }
    case ControllerMessage::Type::RESET_REPLY: {
        *(ack_t*)ptr = static_cast<ack_t>(in.reset_reply.ack);
        break;
    }
    case ControllerMessage::Type::MIGRATION_REQ: {
        *(keyhash_t*)ptr = in.migration_req.keyrange.start;
        ptr += sizeof(keyhash_t);
        *(keyhash_t*)ptr = in.migration_req.keyrange.end;
        ptr += sizeof(keyhash_t);
        *(node_id_t*)ptr = in.migration_req.dst_node_id;
        break;
    }
    case ControllerMessage::Type::MIGRATION_REPLY: {
        *(ack_t*)ptr = static_cast<ack_t>(in.migration_reply.ack);
        break;
    }
    case ControllerMessage::Type::REGISTER_REQ: {
        *(node_id_t*)ptr = in.reg_req.node_id;
        break;
    }
    case ControllerMessage::Type::REGISTER_REPLY: {
        *(keyhash_t*)ptr = in.reg_reply.keyrange.start;
        ptr += sizeof(keyhash_t);
        *(keyhash_t*)ptr = in.reg_reply.keyrange.end;
        ptr += sizeof(keyhash_t);
        break;
    }
    }

    out = string(buf, buf_size);
    delete[] buf;
    return true;
}

bool
ControllerCodec::decode(const std::string &in, ControllerMessage &out)
{
    const char *buf = in.data();
    const char *ptr = buf;
    size_t buf_size = in.size();

    if (buf_size < PACKET_BASE_SIZE) {
        return false;
    }
    if (*(identifier_t *)ptr != IDENTIFIER) {
        return false;
    }
    ptr += sizeof(identifier_t);
    type_t type = *(type_t *)ptr;
    ptr += sizeof(type_t);

    switch(type) {
    case TYPE_RESET_REQ: {
        if (buf_size < RESET_REQ_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::RESET_REQ;
        out.reset_req.num_nodes = *(num_nodes_t*)ptr;
        ptr += sizeof(num_nodes_t);
        out.reset_req.lb_type = static_cast<ControllerResetRequest::LBType>(*(lb_type_t*)ptr);
        break;
    }
    case TYPE_RESET_REPLY: {
        if (buf_size < RESET_REPLY_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::RESET_REPLY;
        out.reset_reply.ack = static_cast<Ack>(*(ack_t*)ptr);
        break;
    }
    case TYPE_MIGRATION_REQ: {
        if (buf_size < MIGRATION_REQ_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::MIGRATION_REQ;
        out.migration_req.keyrange.start = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        out.migration_req.keyrange.end = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        out.migration_req.dst_node_id = *(node_id_t*)ptr;
        break;
    }
    case TYPE_MIGRATION_REPLY: {
        if (buf_size < MIGRATION_REPLY_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::MIGRATION_REPLY;
        out.migration_reply.ack = static_cast<Ack>(*(ack_t*)ptr);
        break;
    }
    case TYPE_REGISTER_REQ: {
        if (buf_size < REGISTER_REQ_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::REGISTER_REQ;
        out.reg_req.node_id = *(node_id_t*)ptr;
        break;
    }
    case TYPE_REGISTER_REPLY: {
        if (buf_size < REGISTER_REPLY_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::REGISTER_REPLY;
        out.reg_reply.keyrange.start = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        out.reg_reply.keyrange.end = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        break;
    }
    default:
        return false;
    }
    return true;
}

} // namespace memcachekv
