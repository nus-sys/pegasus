#ifndef __MEMCACHEKV_MESSAGE_H__
#define __MEMCACHEKV_MESSAGE_H__

#include <string>
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

struct Operation {
    enum Type {
        GET,
        PUT,
        DEL
    };
    Operation()
        : op_type(GET), key(""), value("") {};
    Operation(const proto::Operation &op)
        : op_type(static_cast<Operation::Type>(op.op_type())),
        key(op.key()), value(op.value()) {};

    Type op_type;
    std::string key;
    std::string value;
};

struct MemcacheKVRequest {
    MemcacheKVRequest()
        : client_id(-1), req_id(0) {};
    MemcacheKVRequest(const proto::MemcacheKVRequest &request)
        : client_id(request.client_id()),
        req_id(request.req_id()),
        op(request.op()) {};

    int client_id;
    uint64_t req_id;
    Operation op;
};

enum Result {
    OK,
    NOT_FOUND
};

struct MemcacheKVReply {
    MemcacheKVReply()
        : client_id(-1), req_id(0), result(OK), value("") {};
    MemcacheKVReply(const proto::MemcacheKVReply &reply)
        : client_id(reply.client_id()),
        req_id(reply.req_id()),
        result(static_cast<Result>(reply.result())),
        value(reply.value()) {};

    int client_id;
    uint64_t req_id;
    Result result;
    std::string value;
};

struct MemcacheKVMessage {
    MemcacheKVMessage()
        : has_request(false), has_reply(false) {};

    MemcacheKVRequest request;
    MemcacheKVReply reply;
    bool has_request;
    bool has_reply;
};

class MessageCodec {
public:
    virtual ~MessageCodec() {};

    virtual void decode(const std::string &in, MemcacheKVMessage &out) = 0;
    virtual void encode(std::string &out, const MemcacheKVMessage &in) = 0;
};

class ProtobufCodec : public MessageCodec {
public:
    ProtobufCodec() {};
    ~ProtobufCodec() {};

    void decode(const std::string &in, MemcacheKVMessage &out) override;
    void encode(std::string &out, const MemcacheKVMessage &in) override;
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
