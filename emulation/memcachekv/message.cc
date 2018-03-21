#include "logger.h"
#include "memcachekv/message.h"

using std::string;

namespace memcachekv {

void
ProtobufCodec::decode(const string &in, MemcacheKVMessage &out)
{
    proto::MemcacheKVMessage msg;
    msg.ParseFromString(in);

    if (msg.has_request()) {
        out.has_request = true;
        out.has_reply = false;
        out.request = MemcacheKVRequest(msg.request());
    } else if (msg.has_reply()) {
        out.has_request = false;
        out.has_reply = true;
        out.reply = MemcacheKVReply(msg.reply());
    } else {
        panic("protobuf MemcacheKVMessage wrong format");
    }
}

void
ProtobufCodec::encode(string &out, const MemcacheKVMessage &in)
{
    proto::MemcacheKVMessage msg;

    if (in.has_request) {
        msg.mutable_request()->set_client_id(in.request.client_id);
        msg.mutable_request()->set_req_id(in.request.req_id);
        proto::Operation op;
        op.set_op_type(static_cast<proto::Operation_Type>(in.request.op.op_type));
        op.set_key(in.request.op.key);
        op.set_value(in.request.op.value);
        *(msg.mutable_request()->mutable_op()) = op;
    } else if (in.has_reply) {
        msg.mutable_reply()->set_client_id(in.reply.client_id);
        msg.mutable_reply()->set_req_id(in.reply.req_id);
        msg.mutable_reply()->set_result(static_cast<proto::Result>(in.reply.result));
        msg.mutable_reply()->set_value(in.reply.value);
    } else {
        panic("MemcacheKVMessage wrong format");
    }

    msg.SerializeToString(&out);
}

void
WireCodec::decode(const std::string &in, MemcacheKVMessage &out)
{
    const char *buf = in.data();
    const char *ptr = buf;
    size_t buf_size = in.size();

    // IDENTIFIER
    assert(buf_size > PACKET_BASE_SIZE);
    identifier_t identifier = *(identifier_t *)ptr;
    if (identifier != IDENTIFIER) {
        panic("Wrong packet identifier");
    }
    ptr += sizeof(identifier_t);
    type_t type = *(type_t *)ptr;
    ptr += sizeof(type_t);

    switch (type) {
    case TYPE_REQUEST: {
        assert(buf_size > REQUEST_BASE_SIZE);
        out.has_request = true;
        out.has_reply = false;
        out.request.client_id = (int)*(client_id_t *)ptr;
        ptr += sizeof(client_id_t);
        out.request.req_id = (uint32_t)*(req_id_t *)ptr;
        ptr += sizeof(req_id_t);
        out.request.op.op_type = static_cast<Operation::Type>(*(op_type_t *)ptr);
        ptr += sizeof(op_type_t);
        key_len_t key_len = *(key_len_t *)ptr;
        ptr += sizeof(key_len_t);
        assert(buf_size >= REQUEST_BASE_SIZE + key_len + 1);
        out.request.op.key = string(ptr, key_len);
        ptr += key_len + 1;
        if (out.request.op.op_type == Operation::Type::PUT) {
            assert(buf_size > REQUEST_BASE_SIZE + key_len + 1 + sizeof(value_len_t));
            value_len_t value_len = *(value_len_t *)ptr;
            ptr += sizeof(value_len_t);
            assert(buf_size >= REQUEST_BASE_SIZE + key_len + 1 + sizeof(value_len_t) + value_len + 1);
            out.request.op.value = string(ptr, value_len);
        }
        break;
    }
    case TYPE_REPLY: {
        assert(buf_size > REPLY_BASE_SIZE);
        out.has_request = false;
        out.has_reply = true;
        out.reply.client_id = (int)*(client_id_t *)ptr;
        ptr += sizeof(client_id_t);
        out.reply.req_id = (uint32_t)*(req_id_t *)ptr;
        ptr += sizeof(req_id_t);
        out.reply.result = static_cast<Result>(*(result_t *)ptr);
        ptr += sizeof(result_t);
        value_len_t value_len = *(value_len_t *)ptr;
        ptr += sizeof(value_len_t);
        assert(buf_size > REPLY_BASE_SIZE + value_len + 1);
        out.reply.value = string(ptr, value_len);
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
    if (in.has_request) {
        // +1 for the terminating null
        buf_size = REQUEST_BASE_SIZE + in.request.op.key.size() + 1;
        if (in.request.op.op_type == Operation::Type::PUT) {
            buf_size += sizeof(value_len_t) + in.request.op.value.size() + 1;
        }
    } else if (in.has_reply) {
        buf_size = REPLY_BASE_SIZE + in.reply.value.size() + 1;
    } else {
        panic("Input message wrong format");
    }

    char *buf = new char[buf_size];
    char *ptr = buf;
    *(identifier_t *)ptr = IDENTIFIER;
    ptr += sizeof(identifier_t);
    if (in.has_request) {
        *(type_t *)ptr = TYPE_REQUEST;
        ptr += sizeof(type_t);
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
    } else if (in.has_reply) {
        *(type_t *)ptr = TYPE_REPLY;
        ptr += sizeof(type_t);
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
    }
    out = string(buf, buf_size);
    delete[] buf;
}

} // namespace memcachekv
