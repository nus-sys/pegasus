#include <net/ethernet.h>
#include <netinet/ip.h>
#include <netinet/udp.h>

#include <logger.h>
#include <utils.h>
#include <transports/dpdk/configuration.h>
#include <apps/memcachekv/loadbalancer.h>

typedef tbb::concurrent_hash_map<keyhash_t, std::set<node_t>>::const_accessor const_rset_accessor_t;
typedef tbb::concurrent_hash_map<keyhash_t, std::set<node_t>>::accessor rset_accessor_t;
typedef tbb::concurrent_hash_map<keyhash_t, ver_t>::accessor ver_accessor_t;

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

LoadBalancer::LoadBalancer(Configuration *config)
    : config(config), ver_next(1)
{
    for (node_t i = 0; i < config->node_addresses.at(0).size(); i++) {
        this->all_servers.insert(i);
    }
}

LoadBalancer::~LoadBalancer()
{
}

void LoadBalancer::receive_message(const Message &msg, const Address &addr, int tid)
{
    panic("LoadBalancer should not receive regular message");
}

void LoadBalancer::receive_raw(void *buf, void *tdata, int tid)
{
    PegasusHeader header;
    MetaData data;

    if (!parse_pegasus_header(buf, header)) {
        panic("Wrong header format");
    }
    process_pegasus_header(header, data);
    rewrite_address(buf, data);
    rewrite_pegasus_header(buf, header);
    calculate_chksum(buf);
    this->transport->send_raw(buf, tdata);
}

void LoadBalancer::run()
{
    // Empty
}

void LoadBalancer::run_thread(int tid)
{
    // Empty
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

void LoadBalancer::rewrite_address(void *pkt, struct MetaData &data)
{
    // Currently only support DPDK addresses
    DPDKAddress *addr;
    if (data.is_server) {
        addr = static_cast<DPDKAddress*>(this->config->node_addresses.at(0).at(data.dst));
    } else {
        addr = static_cast<DPDKAddress*>(this->config->client_addresses.at(data.dst));
    }

    char *ptr = (char*)pkt;
    struct ether_header *eth = (struct ether_header*)ptr;
    memcpy(eth->ether_dhost, &addr->ether_addr, ETH_ALEN);
    ptr += ETHER_HDR_LEN;
    struct iphdr *ip = (struct iphdr*)ptr;
    ip->daddr = addr->ip_addr;
    ptr += IPV4_HDR_LEN;
    struct udphdr *udp = (struct udphdr*)ptr;
    udp->dest = addr->udp_port;
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
                                          struct MetaData &data)
{
    switch (header.op_type) {
    case OP_GET:
        handle_read_req(header, data);
        break;
    case OP_PUT:
    case OP_DEL:
        handle_write_req(header, data);
        break;
    case OP_REP_R:
    case OP_REP_W:
        handle_reply(header, data);
        break;
    case OP_MGR_REQ:
        panic("Not implemented");
        break;
    case OP_MGR_ACK:
        panic("Not implemented");
        break;
    case OP_PUT_FWD:
        panic("Not implemented");
        break;
    default:
        panic("Unexpected Pegasus op type %u", header.op_type);
    }
}

void LoadBalancer::handle_read_req(struct PegasusHeader &header, struct MetaData &data)
{
    const_rset_accessor_t ac;

    data.is_server = true;
    if (this->rset.find(ac, header.keyhash)) {
        header.server_id = select_server(ac->second);
    }
    data.dst = header.server_id;
}

void LoadBalancer::handle_write_req(struct PegasusHeader &header, struct MetaData &data)
{
    const_rset_accessor_t ac;

    data.is_server = true;
    header.ver = std::atomic_fetch_add(&this->ver_next, 1);
    if (this->rset.find(ac, header.keyhash)) {
        header.server_id = select_server(this->all_servers);
    }
    data.dst = header.server_id;
}

void LoadBalancer::handle_reply(struct PegasusHeader &header, struct MetaData &data)
{
    rset_accessor_t ac_rset;
    ver_accessor_t ac_ver;

    data.is_server = false;
    data.dst = header.client_id;
    if (this->rset.find(ac_rset, header.keyhash)) {
        this->ver_completed.insert(ac_ver, header.keyhash);
        if (header.ver > ac_ver->second) {
            ac_ver->second = header.ver;
            ac_rset->second.clear();
            ac_rset->second.insert(header.server_id);
        } else if (header.ver == ac_ver->second) {
            ac_rset->second.insert(header.server_id);
        }
    }
}

node_t LoadBalancer::select_server(const std::set<node_t> &servers)
{
    if (servers.empty()) {
        panic("rset is empty");
    }
    auto it = servers.begin();
    for (unsigned long i = 0; i < rand() % servers.size(); i++) {
        it++;
    }
    return *it;
}

} // namespace memcachekv
