#ifndef _MEMCACHEKV_LOADBALANCER_H_
#define _MEMCACHEKV_LOADBALANCER_H_

#include <set>
#include <atomic>
#include <tbb/concurrent_hash_map.h>

#include <application.h>

typedef uint16_t identifier_t;
typedef uint8_t op_type_t;
typedef uint32_t keyhash_t;
typedef uint8_t node_t;
typedef uint16_t load_t;
typedef uint32_t ver_t;

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
    node_t dst;
};

/* Replica set */
typedef struct {
    std::set<node_t> replicas;
    ver_t ver_completed;
} RSetData;

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
    void rewrite_address(void *pkt, struct MetaData &data);
    void calculate_chksum(void *pkt);
    void process_pegasus_header(struct PegasusHeader &header, struct MetaData &data);
    void handle_read_req(struct PegasusHeader &header, struct MetaData &data);
    void handle_write_req(struct PegasusHeader &header, struct MetaData &data);
    void handle_reply(struct PegasusHeader &header, struct MetaData &data);
    void handle_mgr_req(struct PegasusHeader &header, struct MetaData &data);
    void handle_mgr_ack(struct PegasusHeader &header, struct MetaData &data);
    node_t select_server(const std::set<node_t> &servers);

    Configuration *config;
    std::atomic_uint ver_next;
    tbb::concurrent_hash_map<keyhash_t, RSetData> rset;
    std::set<node_t> all_servers;
};

} // namespace memcachekv

#endif /* _MEMCACHEKV_LOADBALANCER_H_ */
