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
#define PEGASUS_IDENTIFIER 0x1573

#define OP_GET      0x0
#define OP_PUT      0x1
#define OP_DEL      0x2
#define OP_REP_R    0x3
#define OP_REP_W    0x4
#define OP_MGR_REQ  0x5
#define OP_MGR_ACK  0x6
#define OP_PUT_FWD  0x7

#define USE_LOCKING

namespace memcachekv {

thread_local static count_t access_count = 0;
thread_local static unsigned set_index = 0;

RSetData::RSetData()
    : ver_completed(0), bitmap(0), size(0)
{
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

RSetData::RSetData(ver_t ver, node_t replica)
{
    reset(ver, replica);
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

RSetData::RSetData(const RSetData &r)
    : ver_completed(r.ver_completed), bitmap(r.bitmap), size(r.size)
{
    for (size_t i = 0; i < r.size; i++) {
        this->replicas[i] = r.replicas[i];
    }
    this->lock = PTHREAD_RWLOCK_INITIALIZER;
}

ver_t RSetData::get_ver_completed() const
{
    return this->ver_completed;
}

node_t RSetData::select() const
{
    return this->replicas[set_index++ % this->size];
}

void RSetData::insert(node_t replica)
{
    if (!(this->bitmap & (1 << replica))) {
        this->bitmap |= (1 << replica);
        this->replicas[this->size++] = replica;
    }
}

void RSetData::reset(ver_t ver, node_t replica)
{
    this->ver_completed = ver;
    this->replicas[0] = replica;
    this->size = 1;
    this->bitmap = 1 << replica;
}

void RSetData::shared_lock()
{
#ifdef USE_LOCKING
    pthread_rwlock_rdlock(&this->lock);
#endif /* USE_LOCKING */
}

void RSetData::exclusive_lock()
{
#ifdef USE_LOCKING
    pthread_rwlock_wrlock(&this->lock);
#endif /* USE_LOCKING */
}

void RSetData::unlock()
{
#ifdef USE_LOCKING
    pthread_rwlock_unlock(&this->lock);
#endif /* USE_LOCKING */
}

LoadBalancer::LoadBalancer(Configuration *config)
    : config(config), ver_next(1)
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
        std::unordered_map<keyhash_t, count_t> uk;
        std::unordered_map<keyhash_t, std::string> uk_keys;
        for (const auto &it : this->hot_ukeys) {
            uk[it.first] = this->ukey_access_count.at(it.first);
            uk_keys[it.first] = it.second;
        }
        std::unordered_map<keyhash_t, count_t> rk;
        for (const auto &it : this->rkeys) {
            // use [] operator because key may not be accessed since last clear
            rk[it.first] = this->rkey_access_count[it.first];
        }

        std::set<std::pair<keyhash_t, count_t>, Comparator> sorted_uk(uk.begin(),
                                                                      uk.end(),
                                                                      comp_desc);
        std::set<std::pair<keyhash_t, count_t>, Comparator> sorted_rk(rk.begin(),
                                                                      rk.end(),
                                                                      comp_asc);
        /* Clear stats */
        pthread_rwlock_wrlock(&this->stats_lock);
        this->rkey_access_count.clear();
        this->ukey_access_count.clear();
        this->hot_ukeys.clear();
        pthread_rwlock_unlock(&this->stats_lock);

        /* Add new rkeys and/or replace old rkeys */
        auto rk_it = sorted_rk.begin();
        for (auto uk_it = sorted_uk.begin();
             uk_it != sorted_uk.end();
             uk_it++) {
            if (this->rkeys.size() < LoadBalancer::MAX_RSET_SIZE) {
                add_rkey(uk_it->first, uk_keys.at(uk_it->first));
            } else if (rk_it != sorted_rk.end() && uk_it->second > rk_it->second) {
                replace_rkey(uk_it->first, uk_keys.at(uk_it->first),
                             rk_it->first, this->rkeys.at(rk_it->first));
                rk_it++;
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
    ptr += sizeof(bitmap_t);

    switch (header.op_type) {
    case OP_GET:
    case OP_PUT:
    case OP_DEL:
        ptr += sizeof(req_id_t);
        ptr += sizeof(req_time_t);
        ptr += sizeof(op_type_t);
        header.key_len = *(key_len_t*)ptr;
        ptr += sizeof(key_len_t);
        header.key = (const char*)ptr;
        break;
    default:
        break;
    }
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
        it->second.shared_lock();
        header.server_id = it->second.select();
        it->second.unlock();
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
        // No lock required: all_servers is read-only
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
        it->second.exclusive_lock();
        ver_t ver = it->second.get_ver_completed();
        if (header.ver > ver) {
            it->second.reset(header.ver, header.server_id);
        } else if (header.ver == ver) {
            it->second.insert(header.server_id);
        }
        it->second.unlock();
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
        it->second.exclusive_lock();
        ver_t ver = it->second.get_ver_completed();
        if (header.ver > ver) {
            it->second.reset(header.ver, header.server_id);
        } else if (header.ver == ver) {
            it->second.insert(header.server_id);
        }
        it->second.unlock();
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
                this->hot_ukeys.insert(std::make_pair(header.keyhash,
                                                      std::string(header.key,
                                                                  header.key_len)));
            }
        }
        pthread_rwlock_unlock(&this->stats_lock);
    }
}

void LoadBalancer::add_rkey(keyhash_t keyhash, const std::string &key)
{
    node_t home = keyhash % this->config->num_nodes;
    RSetData data(0, home);
    this->rkeys.insert(std::make_pair(keyhash, key));
    auto res = this->rset.insert(std::make_pair(keyhash, data));
    if (res.second) {
        // Send ControllerReplication message to home server
        ControllerMessage ctrl;
        ctrl.type = ControllerMessage::Type::REPLICATION;
        ctrl.replication.keyhash = keyhash;
        ctrl.replication.key = key;

        Message msg;
        if (!this->ctrl_codec->encode(msg, ctrl)) {
            panic("Failed to encode ControllerMessage");
        }
        this->transport->send_message_to_local_node(msg, home);
    }
}

void LoadBalancer::replace_rkey(keyhash_t newhash, const std::string &newkey,
                                keyhash_t oldhash, const std::string &oldkey)
{
    if (newhash == oldhash) {
        return;
    }
    assert(this->rkeys.size() > 0);
    this->rkeys.erase(oldhash);
    this->rset.unsafe_erase(oldhash);
    add_rkey(newhash, newkey);
}

} // namespace memcachekv
