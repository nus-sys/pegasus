#include <unordered_map>
#include <cassert>
#include <net/ethernet.h>
#include <netinet/ip.h>
#include <netinet/udp.h>

#include <logger.h>
#include <utils.h>
#include <transports/dpdk/configuration.h>
#include <apps/memcachekv/loadbalancer.h>

#define IPV4_HDR_LEN 20
#define UDP_HDR_LEN 8
#define PEGASUS_IDENTIFIER 0x4750

#define OP_GET      0x0
#define OP_PUT      0x1
#define OP_DEL      0x2
#define OP_REP_R    0x3
#define OP_REP_W    0x4
#define OP_MGR_REQ  0x5
#define OP_MGR_ACK  0x6
#define OP_PUT_FWD  0x7

namespace memcachekv {

thread_local static count_t access_count = 0;
thread_local static unsigned set_index = 0;

static const std::memory_order mem_order = std::memory_order_relaxed;

RSetData::RSetData()
{
    this->ver_completed.store(0, mem_order);
    this->bitmap.store(0, mem_order);
    this->size.store(0, mem_order);
}

RSetData::RSetData(ver_t ver, node_t replica)
{
    reset(ver, replica);
}

RSetData::RSetData(const RSetData &r)
{
    this->ver_completed.store(r.ver_completed.load(mem_order), mem_order);
    this->bitmap.store(r.bitmap.load(mem_order), mem_order);
    this->size.store(r.size.load(mem_order), mem_order);
    for (unsigned i = 0; i < this->size.load(mem_order); i++) {
        this->replicas[i] = r.replicas[i];
    }
}

ver_t RSetData::get_ver_completed() const
{
    return this->ver_completed.load(mem_order);
}

node_t RSetData::select() const
{
    return this->replicas[set_index++ % this->size.load(mem_order)];
}

void RSetData::insert(node_t replica)
{
    unsigned long bm = this->bitmap.fetch_or(1 << replica, mem_order);
    if (!(bm & (1 << replica))) {
        unsigned s = this->size.fetch_add(1, mem_order);
        this->replicas[s] = replica;
    }
}

void RSetData::reset(ver_t ver, node_t replica)
{
    this->ver_completed.store(ver, mem_order);
    this->replicas[0] = replica;
    this->size.store(1, mem_order);
    this->bitmap.store(1 << replica, mem_order);
}

LoadBalancer::LoadBalancer(Configuration *config)
    : config(config), ver_next(1), rset_size(0)
{
    for (node_t i = 0; i < config->node_addresses.at(0).size(); i++) {
        this->all_servers.insert(i);
    }
    this->ctrl_codec = new ControllerCodec();
    this->stats_lock = PTHREAD_RWLOCK_INITIALIZER;
}

LoadBalancer::~LoadBalancer()
{
    delete this->ctrl_codec;
}

void LoadBalancer::receive_message(const Message &msg, const Address &addr, int tid)
{
    panic("LoadBalancer should not receive regular message");
}

bool LoadBalancer::receive_raw(void *buf, void *tdata, int tid)
{
    PegasusHeader header;
    MetaData meta;

    if (!parse_pegasus_header(buf, header)) {
        return false;
    }
    process_pegasus_header(header, meta);
    if (meta.forward) {
        rewrite_address(buf, meta);
        rewrite_pegasus_header(buf, header);
        calculate_chksum(buf);
        this->transport->send_raw(buf, tdata);
        return true;
    } else {
        return false;
    }
}

void LoadBalancer::run()
{
    this->transport->run_app_threads(this);
}

typedef std::function<bool(std::pair<keyhash_t, count_t>,
                           std::pair<keyhash_t, count_t>)> Comparator;
static Comparator comp_desc =
[](std::pair<keyhash_t, count_t> a,
   std::pair<keyhash_t, count_t> b)
{
    return a.second > b.second;
};

static Comparator comp_asc =
[](std::pair<keyhash_t, count_t> a,
   std::pair<keyhash_t, count_t> b)
{
    return a.second < b.second;
};

void LoadBalancer::run_thread(int tid)
{
    while (true) {
        usleep(LoadBalancer::STATS_EPOCH);
        /*
         * Construct hot ukeys sorted in descending order,
         * and rkeys sorted in ascending order
         */
        std::unordered_map<keyhash_t, count_t> ukeys;
        for (const auto &it : this->hot_ukey) {
            ukeys[it.first] = this->ukey_access_count.at(it.first);
        }
        std::set<std::pair<keyhash_t, count_t>, Comparator> sorted_ukey(ukeys.begin(),
                                                                        ukeys.end(),
                                                                        comp_desc);
        std::unordered_map<keyhash_t, count_t> rkeys;
        for (const auto &it : this->rkey_access_count) {
            if (this->rset.count(it.first) > 0) {
                rkeys[it.first] = it.second;
            }
        }
        std::set<std::pair<keyhash_t, count_t>, Comparator> sorted_rkey(rkeys.begin(),
                                                                        rkeys.end(),
                                                                        comp_asc);
        /* Clear stats */
        pthread_rwlock_wrlock(&this->stats_lock);
        this->rkey_access_count.clear();
        this->ukey_access_count.clear();
        this->hot_ukey.clear();
        pthread_rwlock_unlock(&this->stats_lock);
        /* Add new rkeys and/or replace old rkeys */
        auto rkey_it = sorted_rkey.begin();
        for (auto ukey_it = sorted_ukey.begin();
             ukey_it != sorted_ukey.end();
             ukey_it++) {
            if (this->rset_size < LoadBalancer::MAX_RSET_SIZE) {
                add_rkey(ukey_it->first);
            } else if (rkey_it != sorted_rkey.end() && ukey_it->second > rkey_it->second) {
                replace_rkey(ukey_it->first, rkey_it->first);
                rkey_it++;
            } else {
                break;
            }
        }
    }
}

bool LoadBalancer::parse_pegasus_header(const void *pkt, struct PegasusHeader &header)
{
    const char *ptr = (const char*)pkt;

    ptr += ETHER_HDR_LEN;
    ptr += IPV4_HDR_LEN;
    ptr += UDP_HDR_LEN;

    if (*(identifier_t*)ptr != PEGASUS_IDENTIFIER) {
        return false;
    }
    ptr += sizeof(identifier_t);
    header.op_type = *(op_type_t*)ptr;
    ptr += sizeof(op_type_t);
    convert_endian(&header.keyhash, ptr, sizeof(keyhash_t));
    ptr += sizeof(keyhash_t);
    header.client_id = *(node_t*)ptr;
    ptr += sizeof(node_t);
    header.server_id = *(node_t*)ptr;
    ptr += sizeof(node_t);
    convert_endian(&header.load, ptr, sizeof(load_t));
    ptr += sizeof(load_t);
    convert_endian(&header.ver, ptr, sizeof(ver_t));
    ptr += sizeof(ver_t);

    return true;
}

void LoadBalancer::rewrite_pegasus_header(void *pkt, const struct PegasusHeader &header)
{
    char *ptr = (char*)pkt;

    ptr += ETHER_HDR_LEN;
    ptr += IPV4_HDR_LEN;
    ptr += UDP_HDR_LEN;

    ptr += sizeof(identifier_t);
    *(op_type_t*)ptr = header.op_type;
    ptr += sizeof(op_type_t);
    convert_endian(ptr, &header.keyhash, sizeof(keyhash_t));
    ptr += sizeof(keyhash_t);
    *(node_t*)ptr = header.client_id;
    ptr += sizeof(node_t);
    *(node_t*)ptr = header.server_id;
    ptr += sizeof(node_t);
    convert_endian(ptr, &header.load, sizeof(load_t));
    ptr += sizeof(load_t);
    convert_endian(ptr, &header.ver, sizeof(ver_t));
    ptr += sizeof(ver_t);
}

void LoadBalancer::rewrite_address(void *pkt, struct MetaData &meta)
{
    // Currently only support DPDK addresses
    const DPDKAddress *src_addr, *dst_addr;
    src_addr = static_cast<const DPDKAddress*>(this->config->my_address());
    if (meta.is_server) {
        dst_addr = static_cast<DPDKAddress*>(this->config->node_addresses.at(0).at(meta.dst));
    } else {
        dst_addr = static_cast<DPDKAddress*>(this->config->client_addresses.at(meta.dst));
    }

    char *ptr = (char*)pkt;
    struct ether_header *eth = (struct ether_header*)ptr;
    memcpy(eth->ether_shost, &src_addr->ether_addr, ETH_ALEN);
    memcpy(eth->ether_dhost, &dst_addr->ether_addr, ETH_ALEN);
    ptr += ETHER_HDR_LEN;
    struct iphdr *ip = (struct iphdr*)ptr;
    ip->saddr = src_addr->ip_addr;
    ip->daddr = dst_addr->ip_addr;
    ptr += IPV4_HDR_LEN;
    struct udphdr *udp = (struct udphdr*)ptr;
    // Do not rewrite src udp port: we use the sender's random src udp port for
    // RSS
    udp->dest = dst_addr->udp_port;
}

void LoadBalancer::calculate_chksum(void *pkt)
{
    char *ptr = (char*)pkt;
    ptr += ETHER_HDR_LEN;
    /* IP checksum */
    struct iphdr *ip = (struct iphdr*)ptr;
    // clear checksum before calculation
    ip->check = 0;
    uint32_t sum = 0;
    uint16_t *v = (uint16_t*)ptr;
    for (size_t i = 0; i < IPV4_HDR_LEN / sizeof(uint16_t); i++) {
        sum += *v++;
    }
    sum = (sum >> 16) + (sum & 0xFFFF);
    sum += (sum >> 16);
    ip->check = ~sum;
    /* UDP checksum */
    ptr += IPV4_HDR_LEN;
    struct udphdr *udp = (struct udphdr*)ptr;
    udp->check = 0;
}

void LoadBalancer::process_pegasus_header(struct PegasusHeader &header,
                                          struct MetaData &meta)
{
    switch (header.op_type) {
    case OP_GET:
        handle_read_req(header, meta);
        break;
    case OP_PUT:
    case OP_DEL:
        handle_write_req(header, meta);
        break;
    case OP_REP_R:
    case OP_REP_W:
        handle_reply(header, meta);
        break;
    case OP_MGR_REQ:
        handle_mgr_req(header, meta);
        break;
    case OP_MGR_ACK:
        handle_mgr_ack(header, meta);
        break;
    case OP_PUT_FWD:
        panic("Not implemented");
        break;
    default:
        panic("Unexpected Pegasus op type %u", header.op_type);
    }
}

void LoadBalancer::handle_read_req(struct PegasusHeader &header,
                                   struct MetaData &meta)
{
    meta.is_server = true;
    meta.forward = true;
    const auto it = this->rset.find(header.keyhash);
    if (it != this->rset.end()) {
        header.server_id = it->second.select();
        meta.is_rkey = true;
    } else {
        meta.is_rkey = false;
    }
    meta.dst = header.server_id;
    update_stats(header, meta);
}

void LoadBalancer::handle_write_req(struct PegasusHeader &header,
                                    struct MetaData &meta)
{
    meta.is_server = true;
    meta.forward = true;
    header.ver = std::atomic_fetch_add(&this->ver_next, {1});
    const auto it = this->rset.find(header.keyhash);
    if (it != this->rset.end()) {
        header.server_id = this->all_servers.select();
        meta.is_rkey = true;
    } else {
        meta.is_rkey = false;
    }
    meta.dst = header.server_id;
    update_stats(header, meta);
}

void LoadBalancer::handle_reply(struct PegasusHeader &header,
                                struct MetaData &meta)
{
    meta.is_server = false;
    meta.forward = true;
    meta.dst = header.client_id;
    auto it = this->rset.find(header.keyhash);
    if (it != this->rset.end()) {
        ver_t ver = it->second.get_ver_completed();
        if (header.ver > ver) {
            it->second.reset(header.ver, header.server_id);
        } else if (header.ver == ver) {
            it->second.insert(header.server_id);
        }
    }
}

void LoadBalancer::handle_mgr_req(struct PegasusHeader &header,
                                  struct MetaData &meta)
{
    meta.is_server = true;
    meta.forward = true;
    meta.dst = header.server_id;
}

void LoadBalancer::handle_mgr_ack(struct PegasusHeader &header,
                                  struct MetaData &meta)
{
    meta.forward = false;
    auto it = this->rset.find(header.keyhash);
    if (it != this->rset.end()) {
        ver_t ver = it->second.get_ver_completed();
        if (header.ver > ver) {
            it->second.reset(header.ver, header.server_id);
        } else if (header.ver == ver) {
            it->second.insert(header.server_id);
        }
    }
}

void LoadBalancer::update_stats(const struct PegasusHeader &header,
                                const struct MetaData &meta)
{
    if (++access_count % LoadBalancer::STATS_SAMPLE_RATE == 0) {
        pthread_rwlock_rdlock(&this->stats_lock);
        if (meta.is_rkey) {
            ++this->rkey_access_count[header.keyhash];
        } else {
            if (++this->ukey_access_count[header.keyhash] >= LoadBalancer::STATS_HK_THRESHOLD) {
                this->hot_ukey.insert(std::pair<keyhash_t, std::string>(header.keyhash,
                                                                        std::string("")));
            }
        }
        pthread_rwlock_unlock(&this->stats_lock);
    }
}

void LoadBalancer::add_rkey(keyhash_t newkey)
{
    node_t home = newkey % this->config->num_nodes;
    RSetData data(0, home);
    auto res = this->rset.insert(std::pair<keyhash_t, RSetData>(newkey, data));
    if (res.second) {
        this->rset_size++;
        // Send ControllerReplication message to home server
        ControllerMessage ctrl;
        ctrl.type = ControllerMessage::Type::REPLICATION;
        ctrl.replication.keyhash = newkey;
        ctrl.replication.key = std::string("");

        Message msg;
        if (!this->ctrl_codec->encode(msg, ctrl)) {
            panic("Failed to encode ControllerMessage");
        }
        this->transport->send_message_to_local_node(msg, home);
    }
}

void LoadBalancer::replace_rkey(keyhash_t newkey, keyhash_t oldkey)
{
    if (newkey == oldkey) {
        return;
    }
    assert(this->rset_size > 0);
    this->rset.unsafe_erase(oldkey);
    this->rset_size--;
    add_rkey(newkey);
}

} // namespace memcachekv
