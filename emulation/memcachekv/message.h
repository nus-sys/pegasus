#ifndef __MEMCACHEKV_MESSAGE_H__
#define __MEMCACHEKV_MESSAGE_H__

#include <vector>
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
        : client_id(-1), req_id(0), migration_node_id(0) {};
    MemcacheKVRequest(const proto::MemcacheKVRequest &request)
        : client_id(request.client_id()),
        req_id(request.req_id()),
        op(request.op()),
        migration_node_id(0) {};

    int client_id;
    uint32_t req_id;
    Operation op;
    int migration_node_id;
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

struct MigrationRequest {
    std::vector<Operation> ops;
};

struct MemcacheKVMessage {
    enum Type {
        REQUEST,
        REPLY,
        MIGRATION_REQUEST,
        UNKNOWN
    };
    MemcacheKVMessage()
        : type(UNKNOWN) {};

    Type type;
    MemcacheKVRequest request;
    MemcacheKVReply reply;
    MigrationRequest migration_request;
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
     * client_id (32) + req_id (32) + node_id (32) + op_type (8) + key_len (16) + key (+ value_len(16) + value)
     *
     * Reply format:
     * client_id (32) + req_id (32) + result (8) + value_len(16) + value
     *
     * Migration request format:
     * nops (16) + nops * (key_len (16) + key + value_len(16) + value)
     */
    typedef uint32_t identifier_t;
    typedef uint8_t type_t;
    typedef uint32_t client_id_t ;
    typedef uint32_t req_id_t;
    typedef uint32_t node_id_t;
    typedef uint8_t op_type_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;
    typedef uint16_t nops_t;

    static const identifier_t IDENTIFIER = 0xDEADBEEF;
    static const type_t TYPE_REQUEST = 1;
    static const type_t TYPE_REPLY = 2;
    static const type_t TYPE_MIGRATION_REQUEST = 3;
    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);

    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(node_id_t) + sizeof(op_type_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(result_t) + sizeof(value_len_t);
    static const size_t MIGRATION_REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(nops_t);
};

struct ControllerResetMessage {
    enum LBType {
        STATIC,
        ILOAD,
        PLOAD,
        IPLOAD
    };

    int num_nodes;
    LBType lb_type;
};

struct ControllerMessage {
    enum Type {
        RESET
    };
    Type type;
    ControllerResetMessage reset;
};

class ControllerCodec {
    /*
     * Currently only use wire codec for controller
     */
public:
    ControllerCodec() {};
    ~ControllerCodec() {};

    void encode(std::string &out, const ControllerMessage &in);

private:
    /* Wire format:
     * IDENTIFIER (32) + type (8) + message
     *
     * Reset format:
     * num_nodes (32) + lb_type (8)
     */
    typedef uint32_t identifier_t;
    typedef uint8_t type_t;
    typedef uint32_t num_nodes_t;
    typedef uint8_t lb_type_t;

    static const identifier_t IDENTIFIER = 0xDEADDEAD;
    static const type_t TYPE_RESET = 1;
    static const lb_type_t LB_STATIC = 0;
    static const lb_type_t LB_ILOAD = 1;
    static const lb_type_t LB_PLOAD = 2;
    static const lb_type_t LB_IPLOAD = 3;
    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);
    static const size_t RESET_BASE_SIZE = PACKET_BASE_SIZE + sizeof(num_nodes_t) + sizeof(lb_type_t);
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
