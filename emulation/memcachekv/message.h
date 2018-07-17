#ifndef __MEMCACHEKV_MESSAGE_H__
#define __MEMCACHEKV_MESSAGE_H__

#include <list>
#include <string>
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

typedef uint32_t keyhash_t;
typedef uint16_t load_t;

/*
 * KV messages
 */
struct Operation {
    enum class Type {
        GET,
        PUT,
        DEL
    };
    Operation()
        : op_type(Type::GET), key(""), value("") {};
    Operation(const proto::Operation &op)
        : op_type(static_cast<Operation::Type>(op.op_type())),
        key(op.key()), value(op.value()) {};

    Type op_type;
    std::string key;
    std::string value;
};

struct MemcacheKVRequest {
    MemcacheKVRequest()
        : client_id(0), req_id(0) {};
    MemcacheKVRequest(const proto::MemcacheKVRequest &request)
        : client_id(request.client_id()),
        req_id(request.req_id()),
        op(request.op()) {};

    int client_id;
    uint32_t req_id;
    Operation op;
};

enum class Result {
    OK,
    NOT_FOUND
};

struct MemcacheKVReply {
    MemcacheKVReply()
        : node_id(0), load(0), client_id(0), req_id(0), result(Result::OK), value("") {};
    MemcacheKVReply(const proto::MemcacheKVReply &reply)
        : node_id(reply.node_id()),
        client_id(reply.client_id()),
        req_id(reply.req_id()),
        result(static_cast<Result>(reply.result())),
        value(reply.value()) {};

    int node_id;
    load_t load;
    int client_id;
    uint32_t req_id;
    Result result;
    std::string value;
};

struct MigrationRequest {
    std::list<Operation> ops;
};

struct MemcacheKVMessage {
    enum class Type {
        REQUEST,
        REPLY,
        MGR,
        UNKNOWN
    };
    MemcacheKVMessage()
        : type(Type::UNKNOWN) {};

    Type type;
    MemcacheKVRequest request;
    MemcacheKVReply reply;
    MigrationRequest migration_request;
};

class MessageCodec {
public:
    virtual ~MessageCodec() {};

    virtual bool decode(const std::string &in, MemcacheKVMessage &out) = 0;
    virtual bool encode(std::string &out, const MemcacheKVMessage &in) = 0;
};

class ProtobufCodec : public MessageCodec {
public:
    ProtobufCodec() {};
    ~ProtobufCodec() {};

    bool decode(const std::string &in, MemcacheKVMessage &out) override;
    bool encode(std::string &out, const MemcacheKVMessage &in) override;
};

class WireCodec : public MessageCodec {
public:
    WireCodec() {};
    ~WireCodec() {};

    bool decode(const std::string &in, MemcacheKVMessage &out) override;
    bool encode(std::string &out, const MemcacheKVMessage &in) override;

private:
    /* Wire format:
     * identifier (16) + op_type (8) + key_hash (32) + node (8) + load (16) + message
     *
     * Request format:
     * client_id (32) + req_id (32) + key_len (16) + key (+ value_len(16) + value)
     *
     * Reply format:
     * client_id (32) + req_id (32) + result (8) + value_len(16) + value
     *
     * Migration request format:
     * nops (16) + nops * (key_len (16) + key + value_len(16) + value)
     */
    typedef uint16_t identifier_t;
    typedef uint8_t op_type_t;
    typedef uint32_t keyhash_t;
    typedef uint8_t node_t;
    typedef uint16_t load_t;
    typedef uint32_t client_id_t;
    typedef uint32_t req_id_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;
    typedef uint16_t nops_t;

    static const identifier_t PEGASUS = 0x4750;
    static const op_type_t OP_GET   = 0x0;
    static const op_type_t OP_PUT   = 0x1;
    static const op_type_t OP_DEL   = 0x2;
    static const op_type_t OP_REP   = 0x3;
    static const op_type_t OP_MGR   = 0x4;
    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(op_type_t) + sizeof(keyhash_t) + sizeof(node_t) + sizeof(load_t);

    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(result_t) + sizeof(value_len_t);
    static const size_t MGR_BASE_SIZE = PACKET_BASE_SIZE + sizeof(nops_t);
};

/*
 * Controller messages
 */
enum class Ack {
    OK,
    FAILED
};

struct ControllerResetRequest {
    int num_nodes;
};

struct ControllerResetReply {
    Ack ack;
};

struct ControllerHKReport {
    struct Report {
        Report()
            : keyhash(0), load(0) {}
        Report(keyhash_t keyhash, load_t load)
            : keyhash(keyhash), load(load) {}
        keyhash_t keyhash;
        load_t load;
    };
    std::list<Report> reports;
};

struct ControllerMessage {
    enum class Type {
        RESET_REQ,
        RESET_REPLY,
        HK_REPORT
    };
    Type type;
    ControllerResetRequest reset_req;
    ControllerResetReply reset_reply;
    ControllerHKReport hk_report;
};

class ControllerCodec {
    /*
     * Currently only use wire codec for controller
     */
public:
    ControllerCodec() {};
    ~ControllerCodec() {};

    bool encode(std::string &out, const ControllerMessage &in);
    bool decode(const std::string &in, ControllerMessage &out);

private:
    /* Wire format:
     * IDENTIFIER (16) + type (8) + message
     *
     * Reset request:
     * num_nodes (16)
     *
     * Reset reply:
     * ack (8)
     *
     * Hot key report:
     * nkeys (16) + nkeys * (keyhash (32) + load (16))
     */
    typedef uint16_t identifier_t;
    typedef uint8_t type_t;
    typedef uint16_t nnodes_t;
    typedef uint8_t ack_t;
    typedef uint8_t node_t;
    typedef uint16_t nkeys_t;
    typedef uint32_t keyhash_t;
    typedef uint16_t load_t;

    static const identifier_t CONTROLLER = 0xDEAC;

    static const type_t TYPE_RESET_REQ      = 0;
    static const type_t TYPE_RESET_REPLY    = 1;
    static const type_t TYPE_HK_REPORT      = 2;

    static const ack_t ACK_OK       = 0;
    static const ack_t ACK_FAILED   = 1;

    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);
    static const size_t RESET_REQ_SIZE = PACKET_BASE_SIZE + sizeof(nnodes_t);
    static const size_t RESET_REPLY_SIZE = PACKET_BASE_SIZE + sizeof(ack_t);
    static const size_t HK_REPORT_BASE_SIZE = PACKET_BASE_SIZE + sizeof(nkeys_t);
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
