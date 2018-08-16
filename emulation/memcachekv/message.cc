#include "logger.h"
#include "memcachekv/message.h"
#include "memcachekv/config.h"

using std::string;

namespace memcachekv {

static void convert_endian(void *dst, const void *src, size_t size)
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
        msg.mutable_reply()->set_node_id(in.reply.node_id);
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
    if (this->proto_enable && *(identifier_t*)ptr != PEGASUS) {
        return false;
    }
    if (!this->proto_enable && *(identifier_t*)ptr != STATIC) {
        return false;
    }
    ptr += sizeof(identifier_t);
    op_type_t op_type = *(op_type_t*)ptr;
    ptr += sizeof(op_type_t);
    keyhash_t keyhash;
    convert_endian(&keyhash, ptr, sizeof(keyhash_t));
    ptr += sizeof(keyhash_t);
    int node_id = *(node_t*)ptr;
    ptr += sizeof(node_t);
    load_t load;
    convert_endian(&load, ptr, sizeof(load_t));
    ptr += sizeof(load_t);
    ver_t ver;
    convert_endian(&ver, ptr, sizeof(ver_t));
    ptr += sizeof(ver_t);
    int num_replicas = *(node_t*)ptr;
    ptr += sizeof(node_t);
    ptr += sizeof(load_t);

    switch (op_type) {
    case OP_GET:
    case OP_PUT:
    case OP_DEL:
    case OP_MGR: {
        if (buf_size < REQUEST_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REQUEST;
        out.request.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.request.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        switch (op_type) {
        case OP_GET:
            out.request.op.op_type = Operation::Type::GET;
            break;
        case OP_PUT:
            out.request.op.op_type = Operation::Type::PUT;
            break;
        case OP_DEL:
            out.request.op.op_type = Operation::Type::DEL;
            break;
        case OP_MGR:
            out.request.op.op_type = Operation::Type::MGR;
        }
        out.request.op.keyhash = keyhash;
        out.request.op.node_id = node_id;
        out.request.op.ver = ver;
        out.request.op.num_replicas = num_replicas;
        key_len_t key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        if (buf_size < REQUEST_BASE_SIZE + key_len) {
            return false;
        }
        out.request.op.key = string(ptr, key_len);
        ptr += key_len;
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
    case OP_REP_R:
    case OP_REP_W: {
        if (buf_size < REPLY_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REPLY;
        if (op_type == OP_REP_R) {
            out.reply.type = MemcacheKVReply::Type::READ;
        } else {
            out.reply.type = MemcacheKVReply::Type::WRITE;
        }
        out.reply.keyhash = keyhash;
        out.reply.node_id = node_id;
        out.reply.load = load;
        out.reply.ver = ver;
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
    case OP_MGR_REQ: {
        if (buf_size < MGR_REQ_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::MGR_REQ;
        out.migration_request.keyhash = keyhash;
        out.migration_request.ver = ver;
        key_len_t key_len = *(key_len_t *)ptr;
        ptr += sizeof(key_len_t);
        out.migration_request.key = string(ptr, key_len);
        ptr += key_len;
        value_len_t value_len = *(value_len_t *)ptr;
        ptr += sizeof(value_len_t);
        out.migration_request.value = string(ptr, value_len);
        ptr += value_len;
        break;
    }
    case OP_MGR_ACK: {
        panic("Server should never receive MGR_ACK");
        break;
    }
    default:
        return false;
    }
    return true;
}

bool
WireCodec::encode(std::string &out, const MemcacheKVMessage &in)
{
    // First determine buffer size
    size_t buf_size;
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        buf_size = REQUEST_BASE_SIZE + in.request.op.key.size();
        if (in.request.op.op_type == Operation::Type::PUT) {
            buf_size += sizeof(value_len_t) + in.request.op.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        buf_size = REPLY_BASE_SIZE + in.reply.value.size();
        break;
    }
    case MemcacheKVMessage::Type::MGR_REQ: {
        buf_size = MGR_REQ_BASE_SIZE + in.migration_request.key.size() + in.migration_request.value.size();
        break;
    }
    case MemcacheKVMessage::Type::MGR_ACK: {
        buf_size = MGR_ACK_BASE_SIZE;
        break;
    }
    default:
        return false;
    }

    char *buf = new char[buf_size];
    char *ptr = buf;
    // App header
    if (this->proto_enable) {
        *(identifier_t*)ptr = PEGASUS;
    } else {
        *(identifier_t*)ptr = STATIC;
    }
    ptr += sizeof(identifier_t);
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        switch (in.request.op.op_type) {
        case Operation::Type::GET:
            *(op_type_t*)ptr = OP_GET;
            break;
        case Operation::Type::PUT:
            *(op_type_t*)ptr = OP_PUT;
            break;
        case Operation::Type::DEL:
            *(op_type_t*)ptr = OP_DEL;
            break;
        case Operation::Type::MGR:
            panic("Should never generate MGR on a server!");
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        keyhash_t hash = (keyhash_t)compute_keyhash(in.request.op.key);
        hash = hash & KEYHASH_MASK; // controller uses signed int
        convert_endian(ptr, &hash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        *(node_t*)ptr = 0;
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        *(ver_t*)ptr = 0;
        ptr += sizeof(ver_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        switch (in.reply.type) {
        case MemcacheKVReply::Type::READ:
            *(op_type_t*)ptr = OP_REP_R;
            break;
        case MemcacheKVReply::Type::WRITE:
            *(op_type_t*)ptr = OP_REP_W;
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.reply.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        *(node_t*)ptr = in.reply.node_id;
        ptr += sizeof(node_t);
        convert_endian(ptr, &in.reply.load, sizeof(load_t));
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.reply.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        break;
    }
    case MemcacheKVMessage::Type::MGR_REQ: {
        *(op_type_t*)ptr = OP_MGR_REQ;
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.migration_request.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.migration_request.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        break;
    }
    case MemcacheKVMessage::Type::MGR_ACK: {
        *(op_type_t*)ptr = OP_MGR_ACK;
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.migration_ack.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        *(node_t*)ptr = in.migration_ack.node_id;
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.migration_ack.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        break;
    }
    default:
        return false;
    }

    // Payload
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        *(client_id_t*)ptr = (client_id_t)in.request.client_id;
        ptr += sizeof(client_id_t);
        *(req_id_t*)ptr = (req_id_t)in.request.req_id;
        ptr += sizeof(req_id_t);
        *(key_len_t*)ptr = (key_len_t)in.request.op.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.request.op.key.data(), in.request.op.key.size());
        ptr += in.request.op.key.size();
        if (in.request.op.op_type == Operation::Type::PUT) {
            *(value_len_t*)ptr = (value_len_t)in.request.op.value.size();
            ptr += sizeof(value_len_t);
            memcpy(ptr, in.request.op.value.data(), in.request.op.value.size());
            ptr += in.request.op.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        *(client_id_t*)ptr = (client_id_t)in.reply.client_id;
        ptr += sizeof(client_id_t);
        *(req_id_t*)ptr = (req_id_t)in.reply.req_id;
        ptr += sizeof(req_id_t);
        *(result_t *)ptr = (result_t)in.reply.result;
        ptr += sizeof(result_t);
        *(value_len_t *)ptr = (value_len_t)in.reply.value.size();
        ptr += sizeof(value_len_t);
        if (in.reply.value.size() > 0) {
            memcpy(ptr, in.reply.value.data(), in.reply.value.size());
            ptr += in.reply.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::MGR_REQ: {
        *(key_len_t *)ptr = (key_len_t)in.migration_request.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.migration_request.key.data(), in.migration_request.key.size());
        ptr += in.migration_request.key.size();
        *(value_len_t *)ptr = (value_len_t)in.migration_request.value.size();
        ptr += sizeof(value_len_t);
        memcpy(ptr, in.migration_request.value.data(), in.migration_request.value.size());
        ptr += in.migration_request.value.size();
        break;
    }
    case MemcacheKVMessage::Type::MGR_ACK: {
        // emptry
        break;
    }
    default:
        return false;
    }

    out = string(buf, buf_size);
    delete[] buf;
    return true;
}

bool
ControllerCodec::encode(std::string &out, const ControllerMessage &in)
{
    size_t buf_size;
    switch (in.type) {
    case ControllerMessage::Type::RESET_REQ:
        buf_size = RESET_REQ_SIZE;
        break;
    case ControllerMessage::Type::RESET_REPLY:
        buf_size = RESET_REPLY_SIZE;
        break;
    case ControllerMessage::Type::HK_REPORT:
        buf_size = HK_REPORT_BASE_SIZE + in.hk_report.reports.size() * (sizeof(keyhash_t) + sizeof(load_t));
        break;
    default:
        return false;
    }

    char *buf = new char[buf_size];
    char *ptr = buf;
    *(identifier_t*)ptr = CONTROLLER;
    ptr += sizeof(identifier_t);
    *(type_t*)ptr = static_cast<type_t>(in.type);
    ptr += sizeof(type_t);

    switch (in.type) {
    case ControllerMessage::Type::RESET_REQ:
        *(nnodes_t*)ptr = in.reset_req.num_nodes;
        break;
    case ControllerMessage::Type::RESET_REPLY:
        *(ack_t*)ptr = static_cast<ack_t>(in.reset_reply.ack);
        break;
    case ControllerMessage::Type::HK_REPORT:
        *(nkeys_t*)ptr = in.hk_report.reports.size();
        ptr += sizeof(nkeys_t);
        for (const auto &report : in.hk_report.reports) {
            *(keyhash_t*)ptr = report.keyhash;
            ptr += sizeof(keyhash_t);
            *(load_t*)ptr = report.load;
            ptr += sizeof(load_t);
        }
        break;
    }

    out = string(buf, buf_size);
    delete[] buf;
    return true;
}

bool
ControllerCodec::decode(const std::string &in, ControllerMessage &out)
{
    const char *ptr = in.data();
    size_t buf_size = in.size();

    if (buf_size < PACKET_BASE_SIZE) {
        return false;
    }
    if (*(identifier_t*)ptr != CONTROLLER) {
        return false;
    }
    ptr += sizeof(identifier_t);
    type_t type = *(type_t*)ptr;
    ptr += sizeof(type_t);

    switch(type) {
    case TYPE_RESET_REQ:
        if (buf_size < RESET_REQ_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::RESET_REQ;
        out.reset_req.num_nodes = *(nnodes_t*)ptr;
        break;
    case TYPE_RESET_REPLY:
        if (buf_size < RESET_REPLY_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::RESET_REPLY;
        out.reset_reply.ack = static_cast<Ack>(*(ack_t*)ptr);
        break;
    case TYPE_HK_REPORT: {
        if (buf_size < HK_REPORT_BASE_SIZE) {
            return false;
        }
        out.type = ControllerMessage::Type::HK_REPORT;
        out.hk_report.reports.clear();
        nkeys_t nkeys = *(nkeys_t*)ptr;
        ptr += sizeof(nkeys_t);
        for (int i = 0; i < nkeys; i++) {
            ControllerHKReport::Report report;
            report.keyhash = *(keyhash_t*)ptr;
            ptr += sizeof(keyhash_t);
            report.load = *(load_t*)ptr;
            ptr += sizeof(load_t);
            out.hk_report.reports.push_back(report);
        }
        break;
    }
    default:
        return false;
    }
    return true;
}

} // namespace memcachekv
