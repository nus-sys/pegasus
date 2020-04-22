#ifndef __MEMCACHEKV_MESSAGE_H__
#define __MEMCACHEKV_MESSAGE_H__

#include <sys/socket.h>
#include <list>
#include <string>

#include <transport.h>

namespace memcachekv {

typedef uint32_t keyhash_t;
typedef uint16_t load_t;
typedef uint32_t ver_t;
#define KEYHASH_MASK 0x7FFFFFFF;

/*
 * KV messages
 */
enum class OpType {
    GET,
    PUT,
    DEL,
    PUTFWD,
};
struct Operation {
    Operation()
        : op_type(OpType::GET), keyhash(0), ver(0), key(""), value("") {};

    OpType op_type;
    keyhash_t keyhash;
    ver_t ver;

    std::string key;
    std::string value;
};

struct MemcacheKVRequest {
    MemcacheKVRequest()
        : client_id(0), req_id(0), node_id(0) {};

    int client_id;
    uint32_t req_id;
    uint32_t req_time;
    int node_id;
    Operation op;
};

enum class Result {
    OK,
    NOT_FOUND
};

struct MemcacheKVReply {
    MemcacheKVReply()
        : keyhash(0), node_id(0), load(0), ver(0), client_id(0),
        req_id(0), req_time(0), op_type(OpType::GET),
        result(Result::OK), value("") {};

    keyhash_t keyhash;
    int node_id;
    load_t load;
    ver_t ver;

    int client_id;
    uint32_t req_id;
    uint32_t req_time;
    OpType op_type;
    Result result;
    std::string key;
    std::string value;
};

struct MigrationRequest {
    keyhash_t keyhash;
    ver_t ver;

    std::string key;
    std::string value;
};

struct MigrationAck {
    keyhash_t keyhash;
    ver_t ver;
    int node_id;
};

struct MemcacheKVMessage {
    enum class Type {
        REQUEST,
        REPLY,
        MGR_REQ,
        MGR_ACK,
        UNKNOWN
    };
    MemcacheKVMessage()
        : type(Type::UNKNOWN) {};

    Type type;
    MemcacheKVRequest request;
    MemcacheKVReply reply;
    MigrationRequest migration_request;
    MigrationAck migration_ack;
};

class MessageCodec {
public:
    virtual ~MessageCodec() {};

    virtual bool decode(const Message &in, MemcacheKVMessage &out) = 0;
    virtual bool encode(Message &out, const MemcacheKVMessage &in) = 0;
};

class WireCodec : public MessageCodec {
public:
    WireCodec()
        : proto_enable(false) {};
    WireCodec(bool proto_enable)
        : proto_enable(proto_enable) {};
    ~WireCodec() {};

    virtual bool decode(const Message &in, MemcacheKVMessage &out) override final;
    virtual bool encode(Message &out, const MemcacheKVMessage &in) override final;

private:
    bool proto_enable;
    /* Wire format:
     * Header:
     * identifier (16) + op_type (8) + key_hash (32) + node (8) + load (16) +
     * version (32) + debug_node (8) + debug_load (16) + message payload
     *
     * Message payload:
     * Request:
     * client_id (32) + req_id (32) + req_time (32) + op_type (8) + key_len (16)
     * + key (+ value_len(16) + value)
     *
     * Reply:
     * client_id (32) + req_id (32) + req_time (32) + op_type (8) + result (8) +
     * value_len(16) + value
     *
     * Migration request:
     * key_len (16) + key + value_len (16) + value
     *
     * Migration ack:
     * empty
     */
    typedef uint16_t identifier_t;
    typedef uint8_t op_type_t;
    typedef uint32_t keyhash_t;
    typedef uint8_t node_t;
    typedef uint16_t load_t;
    typedef uint32_t ver_t;
    typedef uint32_t client_id_t;
    typedef uint32_t req_id_t;
    typedef uint32_t req_time_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;
    typedef uint16_t sa_family_t;

    static const identifier_t PEGASUS = 0x4750;
    static const identifier_t STATIC = 0x1573;
    static const op_type_t OP_GET       = 0x0;
    static const op_type_t OP_PUT       = 0x1;
    static const op_type_t OP_DEL       = 0x2;
    static const op_type_t OP_REP_R     = 0x3;
    static const op_type_t OP_REP_W     = 0x4;
    static const op_type_t OP_MGR_REQ   = 0x5;
    static const op_type_t OP_MGR_ACK   = 0x6;
    static const op_type_t OP_PUT_FWD   = 0x7;

    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(op_type_t) + sizeof(keyhash_t) + sizeof(node_t) + sizeof(load_t) + sizeof(ver_t) + sizeof(node_t) + sizeof(load_t);
    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(req_time_t) + sizeof(op_type_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(req_time_t) + sizeof(op_type_t) + sizeof(result_t) + sizeof(value_len_t);
    static const size_t MGR_REQ_BASE_SIZE = PACKET_BASE_SIZE + sizeof(key_len_t) + sizeof(value_len_t);
    static const size_t MGR_ACK_BASE_SIZE = PACKET_BASE_SIZE;
};

/*
 * Netcache codec
 */
class NetcacheCodec : public MessageCodec {
public:
    NetcacheCodec() {};
    ~NetcacheCodec() {};

    virtual bool decode(const Message &in, MemcacheKVMessage &out) override final;
    virtual bool encode(Message &out, const MemcacheKVMessage &in) override final;

private:
    /* Wire format:
     * Header:
     * identifier (16) + op_type (8) + key (48) + value (32) + message payload
     *
     * Message payload:
     * Request:
     * client_id (32) + req_id (32) + req_time (32) + op_type (8) + key_len (16)
     * + key (+ value_len(16) + value)
     *
     * Reply:
     * client_id (32) + req_id (32) + req_time (32) + op_type (8) + result (8) +
     * value_len(16) + value
     */
    typedef uint16_t identifier_t;
    typedef uint8_t op_type_t;
    typedef uint32_t client_id_t;
    typedef uint32_t req_id_t;
    typedef uint32_t req_time_t;
    typedef uint16_t key_len_t;
    typedef uint8_t result_t;
    typedef uint16_t value_len_t;
    static const size_t KEY_SIZE        = 6;
    static const size_t VALUE_SIZE      = 4;

    static const identifier_t NETCACHE  = 0x5039;
    static const op_type_t OP_READ      = 0x1;
    static const op_type_t OP_WRITE     = 0x2;
    static const op_type_t OP_REP_R     = 0x3;
    static const op_type_t OP_REP_W     = 0x4;
    static const op_type_t OP_CACHE_HIT = 0x5;

    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(op_type_t) + KEY_SIZE + VALUE_SIZE;
    static const size_t REQUEST_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(req_time_t) + sizeof(op_type_t) + sizeof(key_len_t);
    static const size_t REPLY_BASE_SIZE = PACKET_BASE_SIZE + sizeof(client_id_t) + sizeof(req_id_t) + sizeof(req_time_t) + sizeof(op_type_t) + sizeof(result_t) + sizeof(value_len_t);
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
    int num_rkeys;
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

struct ControllerKeyMigration {
    keyhash_t keyhash;
    std::string key;
};

struct ControllerMessage {
    enum class Type {
        RESET_REQ,
        RESET_REPLY,
        HK_REPORT,
        KEY_MGR
    };
    Type type;
    ControllerResetRequest reset_req;
    ControllerResetReply reset_reply;
    ControllerHKReport hk_report;
    ControllerKeyMigration key_mgr;
};

class ControllerCodec {
public:
    ControllerCodec() {};
    ~ControllerCodec() {};

    bool decode(const Message &in, ControllerMessage &out);
    bool encode(Message &out, const ControllerMessage &in);

private:
    /* Wire format:
     * IDENTIFIER (16) + type (8) + message
     *
     * Reset request:
     * num_nodes (16) + num_rkeys (16)
     *
     * Reset reply:
     * ack (8)
     *
     * Hot key report:
     * nkeys (16) + nkeys * (keyhash (32) + load (16))
     *
     * Key migration:
     * keyhash (32) + key_len (16) + key
     */
    typedef uint16_t identifier_t;
    typedef uint8_t type_t;
    typedef uint16_t nnodes_t;
    typedef uint16_t nrkeys_t;
    typedef uint8_t ack_t;
    typedef uint8_t node_t;
    typedef uint16_t nkeys_t;
    typedef uint32_t keyhash_t;
    typedef uint16_t load_t;
    typedef uint16_t key_len_t;

    static const identifier_t CONTROLLER = 0xDEAC;

    static const type_t TYPE_RESET_REQ      = 0;
    static const type_t TYPE_RESET_REPLY    = 1;
    static const type_t TYPE_HK_REPORT      = 2;
    static const type_t TYPE_KEY_MGR        = 3;

    static const ack_t ACK_OK       = 0;
    static const ack_t ACK_FAILED   = 1;

    static const size_t PACKET_BASE_SIZE = sizeof(identifier_t) + sizeof(type_t);
    static const size_t RESET_REQ_SIZE = PACKET_BASE_SIZE + sizeof(nnodes_t) + sizeof(nrkeys_t);
    static const size_t RESET_REPLY_SIZE = PACKET_BASE_SIZE + sizeof(ack_t);
    static const size_t HK_REPORT_BASE_SIZE = PACKET_BASE_SIZE + sizeof(nkeys_t);
    static const size_t KEY_MGR_BASE_SIZE = PACKET_BASE_SIZE + sizeof(keyhash_t) + sizeof(key_len_t);
};

} // namespace memcachekv

#endif /* __MEMCACHEKV_MESSAGE_H__ */
