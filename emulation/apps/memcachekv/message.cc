#include <cstring>

#include <logger.h>
#include <utils.h>
#include <apps/memcachekv/message.h>
#include <apps/memcachekv/utils.h>

#define BASE_VERSION 1

using std::string;

namespace memcachekv {

bool WireCodec::decode(const Message &in, MemcacheKVMessage &out)
{
    const char *ptr = (const char*)in.buf();
    size_t buf_size = in.len();

    if (buf_size < PACKET_BASE_SIZE) {
        return false;
    }
    if (this->proto_enable && *(identifier_t*)ptr != PEGASUS) {
        return false;
    }
    if (!this->proto_enable && *(identifier_t*)ptr != STATIC) {
        return false;
    }
    // Header
    ptr += sizeof(identifier_t);
    op_type_t op_type = *(op_type_t*)ptr;
    ptr += sizeof(op_type_t);
    keyhash_t keyhash;
    convert_endian(&keyhash, ptr, sizeof(keyhash_t));
    ptr += sizeof(keyhash_t);
    uint8_t client_id = *(node_t*)ptr;
    ptr += sizeof(node_t);
    uint8_t server_id = *(node_t*)ptr;
    ptr += sizeof(node_t);
    load_t load;
    convert_endian(&load, ptr, sizeof(load_t));
    ptr += sizeof(load_t);
    ver_t ver;
    convert_endian(&ver, ptr, sizeof(ver_t));
    ptr += sizeof(ver_t);
    ptr += sizeof(bitmap_t);

    // Payload
    switch (op_type) {
    case OP_GET:
    case OP_PUT:
    case OP_DEL:
    case OP_PUT_FWD: {
        // Request
        if (buf_size < REQUEST_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REQUEST;
        out.request.client_id = client_id;
        out.request.server_id = server_id;
        out.request.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.request.req_time = *(req_time_t*)ptr;
        ptr += sizeof(req_time_t);
        out.request.op.op_type = static_cast<OpType>(*(op_type_t*)ptr);
        ptr += sizeof(op_type_t);
        out.request.op.keyhash = keyhash;
        out.request.op.ver = ver;
        key_len_t key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        if (buf_size < REQUEST_BASE_SIZE + key_len) {
            return false;
        }
        out.request.op.key = string(ptr, key_len);
        ptr += key_len;
        if (op_type == OP_PUT || op_type == OP_PUT_FWD) {
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
        out.reply.client_id = client_id;
        out.reply.server_id = server_id;
        out.reply.keyhash = keyhash;
        out.reply.load = load;
        out.reply.ver = ver;
        out.reply.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.reply.req_time = *(req_time_t*)ptr;
        ptr += sizeof(req_time_t);
        out.reply.op_type = static_cast<OpType>(*(op_type_t*)ptr);
        ptr += sizeof(op_type_t);
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
    case OP_RC_REQ: {
        if (buf_size < RC_REQ_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::RC_REQ;
        out.rc_request.keyhash = keyhash;
        out.rc_request.ver = ver;
        key_len_t key_len = *(key_len_t *)ptr;
        ptr += sizeof(key_len_t);
        out.rc_request.key = string(ptr, key_len);
        ptr += key_len;
        value_len_t value_len = *(value_len_t *)ptr;
        ptr += sizeof(value_len_t);
        out.rc_request.value = string(ptr, value_len);
        ptr += value_len;
        break;
    }
    case OP_RC_ACK: {
        panic("Server should never receive RC_ACK");
        break;
    }
    default:
        return false;
    }
    return true;
}

bool WireCodec::encode(Message &out, const MemcacheKVMessage &in)
{
    // First determine buffer size
    size_t buf_size;
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        buf_size = REQUEST_BASE_SIZE + in.request.op.key.size();
        if (in.request.op.op_type == OpType::PUT ||
            in.request.op.op_type == OpType::PUTFWD) {
            buf_size += sizeof(value_len_t) + in.request.op.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        buf_size = REPLY_BASE_SIZE + in.reply.value.size();
        break;
    }
    case MemcacheKVMessage::Type::RC_REQ: {
        buf_size = RC_REQ_BASE_SIZE + in.rc_request.key.size() + in.rc_request.value.size();
        break;
    }
    case MemcacheKVMessage::Type::RC_ACK: {
        buf_size = RC_ACK_BASE_SIZE;
        break;
    }
    default:
        return false;
    }

    char *buf = (char*)malloc(buf_size);
    char *ptr = buf;
    // Header
    if (this->proto_enable) {
        *(identifier_t*)ptr = PEGASUS;
    } else {
        *(identifier_t*)ptr = STATIC;
    }
    ptr += sizeof(identifier_t);
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        switch (in.request.op.op_type) {
        case OpType::GET:
            *(op_type_t*)ptr = OP_GET;
            break;
        case OpType::PUT:
            *(op_type_t*)ptr = OP_PUT;
            break;
        case OpType::DEL:
            *(op_type_t*)ptr = OP_DEL;
            break;
        case OpType::PUTFWD:
            *(op_type_t*)ptr = OP_PUT_FWD;
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        keyhash_t hash = (keyhash_t)compute_keyhash(in.request.op.key);
        hash = hash & KEYHASH_MASK; // controller uses signed int
        convert_endian(ptr, &hash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        *(node_t*)ptr = in.request.client_id;
        ptr += sizeof(node_t);
        *(node_t*)ptr = in.request.server_id;
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        ver_t base_version = BASE_VERSION;
        convert_endian(ptr, &base_version, sizeof(ver_t));
        ptr += sizeof(ver_t);
        *(bitmap_t*)ptr = 0;
        ptr += sizeof(bitmap_t);
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        switch (in.reply.op_type) {
        case OpType::GET:
            *(op_type_t*)ptr = OP_REP_R;
            break;
        case OpType::PUT:
        case OpType::DEL:
            *(op_type_t*)ptr = OP_REP_W;
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.reply.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        *(node_t*)ptr = in.reply.client_id;
        ptr += sizeof(node_t);
        *(node_t*)ptr = in.reply.server_id;
        ptr += sizeof(node_t);
        convert_endian(ptr, &in.reply.load, sizeof(load_t));
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.reply.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        bitmap_t bitmap = 1 << in.reply.server_id;
        convert_endian(ptr, &bitmap, sizeof(bitmap_t));
        ptr += sizeof(bitmap_t);
        break;
    }
    case MemcacheKVMessage::Type::RC_REQ: {
        *(op_type_t*)ptr = OP_RC_REQ;
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.rc_request.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        ptr += sizeof(node_t);
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.rc_request.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        ptr += sizeof(bitmap_t);
        break;
    }
    case MemcacheKVMessage::Type::RC_ACK: {
        *(op_type_t*)ptr = OP_RC_ACK;
        ptr += sizeof(op_type_t);
        convert_endian(ptr, &in.rc_ack.keyhash, sizeof(keyhash_t));
        ptr += sizeof(keyhash_t);
        ptr += sizeof(node_t);
        *(node_t*)ptr = in.rc_ack.server_id;
        ptr += sizeof(node_t);
        ptr += sizeof(load_t);
        convert_endian(ptr, &in.rc_ack.ver, sizeof(ver_t));
        ptr += sizeof(ver_t);
        bitmap_t bitmap = 1 << in.rc_ack.server_id;
        convert_endian(ptr, &bitmap, sizeof(bitmap_t));
        ptr += sizeof(bitmap_t);
        break;
    }
    default:
        return false;
    }

    // Payload
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        *(req_id_t*)ptr = (req_id_t)in.request.req_id;
        ptr += sizeof(req_id_t);
        *(req_time_t*)ptr = (req_time_t)in.request.req_time;
        ptr += sizeof(req_time_t);
        *(op_type_t*)ptr = static_cast<op_type_t>(in.request.op.op_type);
        ptr += sizeof(op_type_t);
        *(key_len_t*)ptr = (key_len_t)in.request.op.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.request.op.key.data(), in.request.op.key.size());
        ptr += in.request.op.key.size();
        if (in.request.op.op_type == OpType::PUT ||
            in.request.op.op_type == OpType::PUTFWD) {
            *(value_len_t*)ptr = (value_len_t)in.request.op.value.size();
            ptr += sizeof(value_len_t);
            memcpy(ptr, in.request.op.value.data(), in.request.op.value.size());
            ptr += in.request.op.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        *(req_id_t*)ptr = (req_id_t)in.reply.req_id;
        ptr += sizeof(req_id_t);
        *(req_time_t*)ptr = (req_time_t)in.reply.req_time;
        ptr += sizeof(req_time_t);
        *(op_type_t*)ptr = static_cast<op_type_t>(in.reply.op_type);
        ptr += sizeof(op_type_t);
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
    case MemcacheKVMessage::Type::RC_REQ: {
        *(key_len_t *)ptr = (key_len_t)in.rc_request.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.rc_request.key.data(), in.rc_request.key.size());
        ptr += in.rc_request.key.size();
        *(value_len_t *)ptr = (value_len_t)in.rc_request.value.size();
        ptr += sizeof(value_len_t);
        memcpy(ptr, in.rc_request.value.data(), in.rc_request.value.size());
        ptr += in.rc_request.value.size();
        break;
    }
    case MemcacheKVMessage::Type::RC_ACK: {
        // empty
        break;
    }
    default:
        return false;
    }

    out.set_message(buf, buf_size, true);
    return true;
}

bool NetcacheCodec::decode(const Message &in, MemcacheKVMessage &out)
{
    const char *ptr = (const char*)in.buf();
    size_t buf_size = in.len();

    if (buf_size < PACKET_BASE_SIZE) {
        return false;
    }
    if (*(identifier_t*)ptr != NETCACHE) {
        return false;
    }
    ptr += sizeof(identifier_t);
    op_type_t op_type = *(op_type_t*)ptr;
    ptr += sizeof(op_type_t);
    ptr += KEY_SIZE;
    string cached_value = string(ptr, VALUE_SIZE);
    ptr += VALUE_SIZE;

    switch (op_type) {
    case OP_READ:
    case OP_WRITE: {
        if (buf_size < REQUEST_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REQUEST;
        out.request.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.request.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.request.req_time = *(req_time_t*)ptr;
        ptr += sizeof(req_time_t);
        out.request.op.op_type = static_cast<OpType>(*(op_type_t*)ptr);
        ptr += sizeof(op_type_t);
        key_len_t key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        if (buf_size < REQUEST_BASE_SIZE + key_len) {
            return false;
        }
        out.request.op.key = string(ptr, key_len);
        ptr += key_len;
        if (out.request.op.op_type == OpType::PUT) {
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
        out.reply.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.reply.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.reply.req_time = *(req_time_t*)ptr;
        ptr += sizeof(req_time_t);
        out.reply.op_type = static_cast<OpType>(*(op_type_t*)ptr);
        ptr += sizeof(op_type_t);
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
    case OP_CACHE_HIT: {
        if (buf_size < REQUEST_BASE_SIZE) {
            return false;
        }
        out.type = MemcacheKVMessage::Type::REPLY;
        out.reply.client_id = *(client_id_t*)ptr;
        ptr += sizeof(client_id_t);
        out.reply.req_id = *(req_id_t*)ptr;
        ptr += sizeof(req_id_t);
        out.reply.req_time = *(req_time_t*)ptr;
        ptr += sizeof(req_time_t);
        out.reply.op_type = static_cast<OpType>(*(op_type_t*)ptr);
        ptr += sizeof(op_type_t);
        out.reply.result = Result::OK;
        out.reply.value = cached_value;
        break;
    }
    default:
        return false;
    }
    return true;
}

bool NetcacheCodec::encode(Message &out, const MemcacheKVMessage &in)
{
    // First determine buffer size
    size_t buf_size;
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        buf_size = REQUEST_BASE_SIZE + in.request.op.key.size();
        if (in.request.op.op_type == OpType::PUT) {
            buf_size += sizeof(value_len_t) + in.request.op.value.size();
        }
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        buf_size = REPLY_BASE_SIZE + in.reply.value.size();
        break;
    }
    default:
        return false;
    }

    char *buf = (char*)malloc(buf_size);
    char *ptr = buf;
    // App header
    *(identifier_t*)ptr = NETCACHE;
    ptr += sizeof(identifier_t);
    switch (in.type) {
    case MemcacheKVMessage::Type::REQUEST: {
        switch (in.request.op.op_type) {
        case OpType::GET:
            *(op_type_t*)ptr = OP_READ;
            break;
        case OpType::PUT:
        case OpType::DEL:
            *(op_type_t*)ptr = OP_WRITE;
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        if (in.request.op.key.size() > KEY_SIZE) {
            return false;
        }
        memset(ptr, 0, KEY_SIZE);
        memcpy(ptr, in.request.op.key.data(), in.request.op.key.size());
        ptr += KEY_SIZE;
        ptr += VALUE_SIZE;
        break;
    }
    case MemcacheKVMessage::Type::REPLY: {
        switch (in.reply.op_type) {
        case OpType::GET:
            *(op_type_t*)ptr = OP_REP_R;
            break;
        case OpType::PUT:
        case OpType::DEL:
            *(op_type_t*)ptr = OP_REP_W;
            break;
        default:
            return false;
        }
        ptr += sizeof(op_type_t);
        if (in.reply.key.size() > KEY_SIZE) {
            return false;
        }
        memset(ptr, 0, KEY_SIZE);
        memcpy(ptr, in.reply.key.data(), in.reply.key.size());
        ptr += KEY_SIZE;
        memset(ptr, 0, VALUE_SIZE);
        memcpy(ptr, in.reply.value.data(), in.reply.value.size());
        ptr += VALUE_SIZE;
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
        *(req_time_t*)ptr = (req_time_t)in.request.req_time;
        ptr += sizeof(req_time_t);
        *(op_type_t*)ptr = static_cast<op_type_t>(in.request.op.op_type);
        ptr += sizeof(op_type_t);
        *(key_len_t*)ptr = (key_len_t)in.request.op.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.request.op.key.data(), in.request.op.key.size());
        ptr += in.request.op.key.size();
        if (in.request.op.op_type == OpType::PUT) {
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
        *(req_time_t*)ptr = (req_time_t)in.reply.req_time;
        ptr += sizeof(req_time_t);
        *(op_type_t*)ptr = static_cast<op_type_t>(in.reply.op_type);
        ptr += sizeof(op_type_t);
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
    default:
        return false;
    }

    out.set_message(buf, buf_size, true);
    return true;
}

bool ControllerCodec::decode(const Message &in, ControllerMessage &out)
{
    const char *ptr = (const char*)in.buf();
    size_t buf_size = in.len();

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
        ptr += sizeof(nnodes_t);
        out.reset_req.num_rkeys = *(nrkeys_t*)ptr;
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
    case TYPE_REPLICATION: {
        if (buf_size < TYPE_REPLICATION) {
            return false;
        }
        out.type = ControllerMessage::Type::REPLICATION;
        out.replication.keyhash = *(keyhash_t*)ptr;
        ptr += sizeof(keyhash_t);
        key_len_t key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        out.replication.key = string(ptr, key_len);
        break;
    }
    default:
        return false;
    }
    return true;
}

bool ControllerCodec::encode(Message &out, const ControllerMessage &in)
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
    case ControllerMessage::Type::REPLICATION:
        buf_size = REPLICATION_BASE_SIZE + in.replication.key.size();
        break;
    default:
        return false;
    }

    char *buf = (char*)malloc(buf_size);
    char *ptr = buf;
    *(identifier_t*)ptr = CONTROLLER;
    ptr += sizeof(identifier_t);
    *(type_t*)ptr = static_cast<type_t>(in.type);
    ptr += sizeof(type_t);

    switch (in.type) {
    case ControllerMessage::Type::RESET_REQ:
        *(nnodes_t*)ptr = in.reset_req.num_nodes;
        ptr += sizeof(nnodes_t);
        *(nrkeys_t*)ptr = in.reset_req.num_rkeys;
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
    case ControllerMessage::Type::REPLICATION:
        *(keyhash_t*)ptr = in.replication.keyhash;
        ptr += sizeof(keyhash_t);
        *(key_len_t*)ptr = in.replication.key.size();
        ptr += sizeof(key_len_t);
        memcpy(ptr, in.replication.key.data(), in.replication.key.size());
        break;
    }

    out.set_message(buf, buf_size, true);
    return true;
}

} // namespace memcachekv
