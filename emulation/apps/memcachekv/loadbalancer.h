#ifndef _MEMCACHEKV_LOADBALANCER_H_
#define _MEMCACHEKV_LOADBALANCER_H_

#include <set>
#include <atomic>
#include <shared_mutex>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_unordered_set.h>

#include <application.h>
#include <apps/memcachekv/message.h>

typedef uint16_t identifier_t;
typedef uint8_t op_type_t;
typedef uint32_t keyhash_t;
typedef uint8_t node_t;
typedef uint16_t load_t;
typedef uint32_t ver_t;

typedef uint64_t count_t;

namespace memcachekv {

/* Pegasus header */
struct PegasusHeader {
    op_type_t op_type;
    keyhash_t keyhash;
    node_t client_id;
    node_t server_id;
    load_t load;
    ver_t ver;
};

/* Process pipeline metadata */
struct MetaData {
    bool is_server;
    bool forward;
    bool is_rkey;
    node_t dst;
};

#define MAX_REPLICAS 32
/* Replica set */
class RSetData {
public:
    RSetData();
    RSetData(ver_t ver, node_t replica);
    RSetData(const RSetData &r);
    ver_t get_ver_completed() const;
    node_t select() const;
    void insert(node_t replica);
    void reset(ver_t ver, node_t replica);

private:
    std::atomic<unsigned> ver_completed;
    std::atomic<unsigned long> bitmap;
    std::atomic<unsigned> size;
    node_t replicas[MAX_REPLICAS];
};

class LoadBalancer : public Application {
public:
    LoadBalancer(Configuration *config);
    ~LoadBalancer();

    virtual void receive_message(const Message &msg,
                                 const Address &addr,
                                 int tid) override final;
    virtual bool receive_raw(void *buf, void *tdata, int tid) override final;
    virtual void run() override final;
    virtual void run_thread(int tid) override final;

private:
    bool parse_pegasus_header(const void *pkt, struct PegasusHeader &header);
    void rewrite_pegasus_header(void *pkt, const struct PegasusHeader &header);
    void rewrite_address(void *pkt, struct MetaData &meta);
    void calculate_chksum(void *pkt);
    void process_pegasus_header(struct PegasusHeader &header,
                                struct MetaData &meta);
    void handle_read_req(struct PegasusHeader &header,
                         struct MetaData &meta);
    void handle_write_req(struct PegasusHeader &header,
                          struct MetaData &meta);
    void handle_reply(struct PegasusHeader &header,
                      struct MetaData &meta);
    void handle_mgr_req(struct PegasusHeader &header,
                        struct MetaData &meta);
    void handle_mgr_ack(struct PegasusHeader &header,
                        struct MetaData &meta);
    void update_stats(const struct PegasusHeader &header,
                      const struct MetaData &meta);
    void add_rkey(keyhash_t newkey);
    void replace_rkey(keyhash_t newkey, keyhash_t oldkey);

    Configuration *config;
    ControllerCodec *ctrl_codec;
    std::atomic_uint ver_next;
    static const size_t MAX_RSET_SIZE = 32;
    tbb::concurrent_unordered_map<keyhash_t, RSetData> rset;
    size_t rset_size;
    RSetData all_servers;

    std::shared_mutex stats_mutex;
    tbb::concurrent_unordered_map<keyhash_t, count_t> rkey_access_count;
    tbb::concurrent_unordered_map<keyhash_t, count_t> ukey_access_count;
    tbb::concurrent_unordered_map<keyhash_t, std::string> hot_ukey;
    static const int STATS_SAMPLE_RATE = 1000;
    static const int STATS_HK_THRESHOLD = 5;
    static const int STATS_EPOCH = 10000;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_LOADBALANCER_H_ */
