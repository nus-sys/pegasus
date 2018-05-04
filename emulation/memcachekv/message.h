#ifndef __MEMCACHEKV_MESSAGE_H__
#define __MEMCACHEKV_MESSAGE_H__

#include <vector>
#include <string>
#include "memcachekv/memcachekv.pb.h"

namespace memcachekv {

typedef uint32_t keyhash_t;

struct KeyRange {
    keyhash_t start;
    keyhash_t end;
};
// Comparator for KeyRange
struct KeyRangeComparator {
    bool operator() (const KeyRange& lhs, const KeyRange& rhs) const
    {
        return lhs.start < rhs.start;
    }
};

struct Operation {
    enum class Type {
        GET,
        PUT,
        DEL
    };
    Operation()
        : op_type(Type::GET), key(""), keyhash(0), value("") {};
    Operation(const proto::Operation &op)
        : op_type(static_cast<Operation::Type>(op.op_type())),
        key(op.key()), value(op.value()) {};

    Type op_type;
    std::string key;
    keyhash_t keyhash;
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

enum class Result {
    OK,
    NOT_FOUND
};

struct MemcacheKVReply {
    MemcacheKVReply()
        : client_id(-1), req_id(0), result(Result::OK), value("") {};
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
    KeyRange keyrange;
    std::vector<Operation> ops;
};

struct MemcacheKVMessage {
    enum class Type {
        REQUEST,
        REPLY,
        MIGRATION_REQUEST,
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
     * type (8) + rsvd (8) + port (16) + key_hash (32) + message
     *
     * Request format:
     * client_id (32) + req_id (32) + op_type (8) + key_len (16) + key (+ value_len(16) + value)
     *
     * Reply format:
     * client_id (32) + req_id (32) + result (8) + value_len(16) + value
     *
     * Migration request format:
     * range_start (32) + range_end (32) + nops (16) + nops * (key_len (16) + key + value_len(16) + value)
     */
    typedef uint8_t type_t;
    typedef uint8_t rsvd_t;
    typedef uint16_t port_t;
    typedef uint32_t keyhash_t;
    typedef uint32_t client_id_t ;
    typedef uint32_t req_id_t;
    typedef uint8_t op_type_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;
    typedef uint16_t nops_t;

    static const type_t TYPE_REQUEST = 0;
    static const type_t TYPE_REPLY = 1;
    static const type_t TYPE_MIGRATION_REQUEST = 2;
    static const size_t PACKET_BASE_SIZE = sizeof(type_t) + sizeof(rsvd_t) + sizeof(port_t) + sizeof(keyhash_t);

    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(op_type_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) +
        sizeof(result_t) + sizeof(value_len_t);
    static const size_t MIGRATION_REQUEST_BASE_SIZE = PACKET_BASE_SIZE + 2 * sizeof(keyhash_t) + sizeof(nops_t);
};

enum class Ack {
    OK,
    FAILED
};

struct ControllerResetRequest {
    enum class LBType {
        STATIC,
        ILOAD,
        PLOAD,
        IPLOAD
    };

    int num_nodes;
    LBType lb_type;
};

struct ControllerResetReply {
    Ack ack;
};

struct ControllerMigrationRequest {
    KeyRange keyrange;
    int dst_node_id;
};

struct ControllerMigrationReply {
    Ack ack;
};

struct ControllerRegisterRequest {
    int node_id;
};

struct ControllerRegisterReply {
    KeyRange keyrange;
};

struct ControllerMessage {
    enum class Type {
        RESET_REQ,
        RESET_REPLY,
        MIGRATION_REQ,
        MIGRATION_REPLY,
        REGISTER_REQ,
        REGISTER_REPLY
    };
    Type type;
    ControllerResetRequest reset_req;
    ControllerResetReply reset_reply;
    ControllerMigrationRequest migration_req;
    ControllerMigrationReply migration_reply;
    ControllerRegisterRequest reg_req;
    ControllerRegisterReply reg_reply;
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
     * IDENTIFIER (32) + type (8) + message
     *
     * Reset format:
     * num_nodes (32) + lb_type (8)
     *
     * Reset Reply format:
     * ack (8)
     *
     * Migration request format:
     * range_start (32) + range_end (32) + dst_node_id (32)
     *
     * Migration reply format:
     * ack (8)
     *
     * Register request format:
     * node_id (32)
     *
     * Register reply format:
     * range_start (32) + range_end (32)
     */
    typedef uint32_t identifier_t;
    typedef uint8_t type_t;
    typedef uint32_t num_nodes_t;
    typedef uint8_t lb_type_t;
    typedef uint8_t ack_t;
    typedef uint32_t keyhash_t;
    typedef uint32_t node_id_t;

    static const identifier_t IDENTIFIER = 0xDEADDEAD;

    static const type_t TYPE_RESET_REQ          = 0;
    static const type_t TYPE_RESET_REPLY        = 1;
    static const type_t TYPE_MIGRATION_REQ      = 2;
    static const type_t TYPE_MIGRATION_REPLY    = 3;
    static const type_t TYPE_REGISTER_REQ       = 4;
    static const type_t TYPE_REGISTER_REPLY     = 5;

    static const lb_type_t LB_STATIC    = 0;
    static const lb_type_t LB_ILOAD     = 1;
    static const lb_type_t LB_PLOAD     = 2;
    static const lb_type_t LB_IPLOAD    = 3;

    static const ack_t ACK_OK       = 0;
    static const ack_t ACK_FAILED   = 1;

    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);
    static const size_t RESET_REQ_SIZE = PACKET_BASE_SIZE + sizeof(num_nodes_t) + sizeof(lb_type_t);
    static const size_t RESET_REPLY_SIZE = PACKET_BASE_SIZE + sizeof(ack_t);
    static const size_t MIGRATION_REQ_SIZE = PACKET_BASE_SIZE + 2 * sizeof(keyhash_t) + sizeof(node_id_t);
    static const size_t MIGRATION_REPLY_SIZE = PACKET_BASE_SIZE + sizeof(ack_t);
    static const size_t REGISTER_REQ_SIZE = PACKET_BASE_SIZE + sizeof(node_id_t);
    static const size_t REGISTER_REPLY_SIZE = PACKET_BASE_SIZE + 2 * sizeof(keyhash_t);
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
