#include <tofino/intrinsic_metadata.p4>
#include <tofino/constants.p4>

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

#define ETHERTYPE_IPV4  0x800
#define PROTO_UDP       0x11
#define PEGASUS_ID      0x5047
#define HASH_MASK       0x3 // Max 4 nodes
#define OP_GET          0x0
#define OP_PUT          0x1
#define OP_DEL          0x2
#define OP_REP          0x3


header_type ethernet_t {
    fields {
        dstAddr : 48;
        srcAddr : 48;
        etherType : 16;
    }
}


header ethernet_t ethernet;

header_type ipv4_t {
    fields {
        version : 4;
        ihl : 4;
        diffserv : 8;
        totalLen : 16;
        identification : 16;
        flags : 3;
        fragOffset : 13;
        ttl : 8;
        protocol : 8;
        hdrChecksum : 16;
        srcAddr : 32;
        dstAddr : 32;
    }
}

header ipv4_t ipv4;

header_type udp_t {
    fields {
        srcPort : 16;
        dstPort : 16;
        len : 16;
        checksum : 16;
    }
}

header udp_t udp;

header_type apphdr_t {
    fields {
        id : 16;
    }
}

header apphdr_t apphdr;

header_type pegasus_t {
    fields {
        op : 8;
        keyhash : 32;
        node : 8;
        load : 16;
    }
}

header pegasus_t pegasus;

header_type metadata_t {
    fields {
        dst_node : 8;
        total_node_load : 16;
        min_node : 8;
        min_node_load : 16;
        rkey_index : 32;
        n_replicas : 4;
        rnode_1 : 8;
        rnode_2 : 8;
        rnode_3 : 8;
        rnode_4 : 8;
    }
}

metadata metadata_t meta;

/*************************************************************************
*********************** STATEFUL MEMORY  *********************************
*************************************************************************/
register reg_min_node {
    width : 8;
    instance_count : 1;
}
register reg_min_node_load {
    width : 16;
    instance_count : 1;
}
register reg_nreps {
    width : 4;
    instance_count : 32;
}
register reg_rnode1 {
    width : 8;
    instance_count : 32;
}
register reg_rnode2 {
    width : 8;
    instance_count : 32;
}
register reg_rnode3 {
    width : 8;
    instance_count : 32;
}
register reg_rnode4 {
    width : 8;
    instance_count : 32;
}
register reg_total_node_load {
    width : 16;
    instance_count : 1;
}
register reg_node_load {
    width : 16;
    instance_count : 32;
}
/*************************************************************************
*********************** CHECKSUM *****************************************
*************************************************************************/

field_list ipv4_field_list {
    ipv4.version;
    ipv4.ihl;
    ipv4.diffserv;
    ipv4.totalLen;
    ipv4.identification;
    ipv4.flags;
    ipv4.fragOffset;
    ipv4.ttl;
    ipv4.protocol;
    ipv4.srcAddr;
    ipv4.dstAddr;
}

field_list_calculation ipv4_chksum_calc {
    input {
        ipv4_field_list;
    }
    algorithm : csum16;
    output_width: 16;
}

calculated_field ipv4.hdrChecksum {
    update ipv4_chksum_calc;
}

/*************************************************************************
*********************** P A R S E R S  ***********************************
*************************************************************************/

parser start {
    return parse_ethernet;
}

parser parse_ethernet {
    extract(ethernet);
    return select(latest.etherType) {
        ETHERTYPE_IPV4: parse_ipv4;
        default: ingress;
    }
}

parser parse_ipv4 {
    extract(ipv4);
    return select(latest.protocol) {
        PROTO_UDP: parse_udp;
        default: ingress;
    }
}

parser parse_udp {
    extract(udp);
    return parse_apphdr;
}

parser parse_apphdr {
    extract(apphdr);
    return select(latest.id) {
        PEGASUS_ID: parse_pegasus;
        default: ingress;
    }
}

parser parse_pegasus {
    extract(pegasus);
    return ingress;
}

/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

action nop() {
}

action _drop() {
    drop();
}

action l2_forward(port) {
    modify_field(ig_intr_md_for_tm.ucast_egress_port, port);
}

table tab_l2_forward {
    reads {
        ethernet.dstAddr: exact;
    }
    actions {
        l2_forward;
        _drop;
    }
    size : 1024;
}

action node_forward(mac_addr, ip_addr, port) {
    modify_field(ethernet.dstAddr, mac_addr);
    modify_field(ipv4.dstAddr, ip_addr);
    modify_field(ig_intr_md_for_tm.ucast_egress_port, port);
}

table tab_node_forward {
    reads {
        meta.dst_node: exact;
    }
    actions {
        node_forward;
        _drop;
    }
    size : 32;
}

action lookup_min_rnode () {
    modify_field(meta.dst_node, meta.rnode_1);
}

table tab_lookup_min_rnode {
    actions {
        lookup_min_rnode;
    }
}

action init_rkey() {
    register_write(reg_rnode1, meta.rkey_index, pegasus.keyhash & HASH_MASK);
    register_write(reg_nreps, meta.rkey_index, 1);
    modify_field(meta.n_replicas, 1);
    modify_field(meta.rnode_1, pegasus.keyhash & HASH_MASK);
}

table tab_init_rkey {
    actions {
        init_rkey;
    }
}

action extract_rnodes() {
    register_read(meta.n_replicas, reg_nreps, meta.rkey_index);
    register_read(meta.rnode_1, reg_rnode1, meta.rkey_index);
    register_read(meta.rnode_2, reg_rnode2, meta.rkey_index);
    register_read(meta.rnode_3, reg_rnode3, meta.rkey_index);
    register_read(meta.rnode_4, reg_rnode4, meta.rkey_index);
}

table tab_extract_rnodes {
    actions {
        extract_rnodes;
    }
}

action lookup_rkey(rkey_index) {
    modify_field(meta.rkey_index, rkey_index);
}

action default_dst_node() {
    modify_field(meta.dst_node, pegasus.keyhash & HASH_MASK);
}

table tab_replicated_keys {
    reads {
        pegasus.keyhash: exact;
    }
    actions {
        lookup_rkey;
        default_dst_node;
    }
    size : 32;
}

action update_min_node_load() {
    register_write(reg_min_node, 0, pegasus.node);
    register_write(reg_min_node_load, 0, pegasus.load);
}

table tab_update_min_node_load {
    actions {
        update_min_node_load;
    }
}

action update_node_load() {
    register_write(reg_node_load, pegasus.node, pegasus.load);
}

table tab_update_node_load {
    actions {
        update_node_load;
    }
}

action read_node_load_stats() {
    register_read(meta.total_node_load, reg_total_node_load, 0);
    register_read(meta.min_node, reg_min_node, 0);
    register_read(meta.min_node_load, reg_min_node_load, 0);
}

table tab_read_node_load_stats {
    actions {
        read_node_load_stats;
    }
}

control ingress {
    if (valid(pegasus)) {
        apply(tab_read_node_load_stats);
        if (pegasus.op == OP_REP) {
            apply(tab_update_node_load);
            // Update min node load
            if (pegasus.load < meta.min_node_load or pegasus.node == meta.min_node) {
                apply(tab_update_min_node_load);
            }
        } else {
            apply(tab_replicated_keys) {
                hit {
                    apply(tab_extract_rnodes);
                    if (meta.n_replicas == 0) {
                        apply(tab_init_rkey);
                    }
                    apply(tab_lookup_min_rnode);
                }
            }
        }
        // Forward reply messages using L2, all other
        // operations are forwarded based on node.
        if (pegasus.op == OP_REP) {
            apply(tab_l2_forward);
        } else {
            apply(tab_node_forward);
        }
    } else {
        apply(tab_l2_forward);
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control egress {
}
