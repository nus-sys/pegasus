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
    uint32_t req_id;
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
    uint32_t req_id;
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

class WireCodec : public MessageCodec {
public:
    WireCodec() {};
    ~WireCodec() {};

    void decode(const std::string &in, MemcacheKVMessage &out) override;
    void encode(std::string &out, const MemcacheKVMessage &in) override;

private:
    /* Wire format:
     * IDENTIFIER (32) + type (8) + message
     *
     * Request format:
     * client_id (32) + req_id (32) + op_type (8) + key_len (16) + key (+ value_len(16) + value)
     *
     * Reply format:
     * client_id (32) + req_id (32) + result (8) + value_len(16) + value
     */
    typedef uint32_t identifier_t;
    typedef uint8_t type_t;
    typedef uint32_t client_id_t ;
    typedef uint32_t req_id_t;
    typedef uint8_t op_type_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;

    static const identifier_t IDENTIFIER = 0xDEADBEEF;
    static const type_t TYPE_REQUEST = 1;
    static const type_t TYPE_REPLY = 2;
    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);

    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(op_type_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(result_t) + sizeof(value_len_t);
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
