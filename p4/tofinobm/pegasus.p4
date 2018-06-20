#include <tofino/intrinsic_metadata.p4>
#include <tofino/constants.p4>

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

#define ETHERTYPE_IPV4  0x800
#define PROTO_UDP       0x11
#define PEGASUS_ID      0x5047
#define HASH_MASK       0x3 // Max 4 nodes
#define MAX_REPLICAS    0x4
#define OP_GET          0x0
#define OP_PUT          0x1
#define OP_DEL          0x2
#define OP_REP          0x3
#define AVG_LOAD_SHIFT  0x2

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
        // Node load stats
        total_node_load : 16;
        min_node : 8;
        min_node_load : 16;
        // Replica nodes
        rkey_index : 32;
        n_replicas : 4;
        rnode_1 : 8;
        rnode_2 : 8;
        rnode_3 : 8;
        rnode_4 : 8;
        rload_1 : 16;
        rload_2 : 16;
        rload_3 : 16;
        rload_4 : 16;
        min_rload : 16;
        // Temporary variables
        tmp_node : 8;
        tmp_load : 16;
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
        pegasus.node: exact;
    }
    actions {
        node_forward;
        _drop;
    }
    size : 32;
}

action extend_rset4() {
    register_write(reg_nreps, meta.rkey_index, 4);
    register_write(reg_rnode4, meta.rkey_index, meta.min_node);
}

action extend_rset3() {
    register_write(reg_nreps, meta.rkey_index, 3);
    register_write(reg_rnode3, meta.rkey_index, meta.min_node);
}

action extend_rset2() {
    register_write(reg_nreps, meta.rkey_index, 2);
    register_write(reg_rnode2, meta.rkey_index, meta.min_node);
}

table tab_extend_rset {
    reads {
        meta.n_replicas: exact;
    }
    actions {
        extend_rset2;
        extend_rset3;
        extend_rset4;
        _drop;
    }
    size : 4;
}

action update_min_rnode4 () {
    modify_field(pegasus.node, meta.rnode_4);
    modify_field(meta.min_rload, meta.rload_4);
}

table tab_update_min_rnode4 {
    actions {
        update_min_rnode4;
    }
}

action update_min_rnode3 () {
    modify_field(pegasus.node, meta.rnode_3);
    modify_field(meta.min_rload, meta.rload_3);
}

table tab_update_min_rnode3 {
    actions {
        update_min_rnode3;
    }
}

action update_min_rnode2 () {
    modify_field(pegasus.node, meta.rnode_2);
    modify_field(meta.min_rload, meta.rload_2);
}

table tab_update_min_rnode2 {
    actions {
        update_min_rnode2;
    }
}

action update_min_rnode1 () {
    modify_field(pegasus.node, meta.rnode_1);
    modify_field(meta.min_rload, meta.rload_1);
}

table tab_update_min_rnode1 {
    actions {
        update_min_rnode1;
    }
}

action init_rkey() {
    register_write(reg_rnode1, meta.rkey_index, pegasus.keyhash & HASH_MASK);
    register_write(reg_nreps, meta.rkey_index, 1);
    modify_field(meta.n_replicas, 1);
    modify_field(meta.rnode_1, pegasus.keyhash & HASH_MASK);
    register_read(meta.rload_1, reg_node_load, pegasus.keyhash & HASH_MASK);
}

table tab_init_rkey {
    actions {
        init_rkey;
    }
}

action extract_rloads() {
    register_read(meta.rload_1, reg_node_load, meta.rnode_1);
    register_read(meta.rload_2, reg_node_load, meta.rnode_2);
    register_read(meta.rload_3, reg_node_load, meta.rnode_3);
    register_read(meta.rload_4, reg_node_load, meta.rnode_4);
}

table tab_extract_rloads {
    actions {
        extract_rloads;
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
    modify_field(pegasus.node, pegasus.keyhash & HASH_MASK);
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

action update_min_node_load3() {
    modify_field(meta.min_node, 3);
    modify_field(meta.min_node_load, meta.tmp_load);
}

table tab_update_min_node_load3 {
    actions {
        update_min_node_load3;
    }
}

action check_min_node_load3() {
    register_read(meta.tmp_load, reg_node_load, 3);
}

table tab_check_min_node_load3 {
    actions {
        check_min_node_load3;
    }
}

action update_min_node_load2() {
    modify_field(meta.min_node, 2);
    modify_field(meta.min_node_load, meta.tmp_load);
}

table tab_update_min_node_load2 {
    actions {
        update_min_node_load2;
    }
}

action check_min_node_load2() {
    register_read(meta.tmp_load, reg_node_load, 2);
}

table tab_check_min_node_load2 {
    actions {
        check_min_node_load2;
    }
}

action update_min_node_load1() {
    modify_field(meta.min_node, 1);
    modify_field(meta.min_node_load, meta.tmp_load);
}

table tab_update_min_node_load1 {
    actions {
        update_min_node_load1;
    }
}

action check_min_node_load1() {
    register_read(meta.tmp_load, reg_node_load, 1);
}

table tab_check_min_node_load1 {
    actions {
        check_min_node_load1;
    }
}

action check_min_node_load0() {
    register_read(meta.min_node_load, reg_node_load, 0);
    modify_field(meta.min_node, 0);
}

table tab_check_min_node_load0 {
    actions {
        check_min_node_load0;
    }
}

action update_min_node_load_from_meta() {
    register_write(reg_min_node, 0, meta.min_node);
    register_write(reg_min_node_load, 0, meta.min_node_load);
}

table tab_update_min_node_load_from_meta {
    actions {
        update_min_node_load_from_meta;
    }
}

action update_min_node_load_from_hdr() {
    register_write(reg_min_node, 0, pegasus.node);
    register_write(reg_min_node_load, 0, pegasus.load);
}

table tab_update_min_node_load_from_hdr {
    actions {
        update_min_node_load_from_hdr;
    }
}

action dec_total_node_load() {
    register_write(reg_total_node_load, 0, meta.total_node_load - 1);
}

action inc_total_node_load() {
    register_write(reg_total_node_load, 0, meta.total_node_load + 1);
}

table tab_update_total_node_load {
    reads {
        pegasus.op : exact;
    }
    actions {
        inc_total_node_load;
        dec_total_node_load;
    }
    size : 1;
}

action dec_node_load() {
    register_write(reg_node_load, pegasus.node, pegasus.load - 1);
    modify_field(pegasus.load, pegasus.load - 1);
}

action inc_node_load() {
    register_write(reg_node_load, pegasus.node, pegasus.load + 1);
    modify_field(pegasus.load, pegasus.load + 1);
}

table tab_update_node_load {
    reads {
        pegasus.op : exact;
    }
    actions {
        inc_node_load;
        dec_node_load;
    }
    size : 1;
}

action read_node_load() {
    register_read(pegasus.load, reg_node_load, pegasus.node);
}

table tab_read_node_load {
    actions {
        read_node_load;
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
        if (pegasus.op != OP_REP) {
            apply(tab_replicated_keys) {
                hit {
                    apply(tab_extract_rnodes);
                    apply(tab_extract_rloads);
                    if (meta.n_replicas == 0) {
                        apply(tab_init_rkey);
                    }
                    // Find replica with the min load
                    apply(tab_update_min_rnode1);
                    if (meta.n_replicas - 1 > 0) {
                        if (meta.rload_2 < meta.min_rload) {
                            apply(tab_update_min_rnode2);
                        }
                        if (meta.n_replicas - 2 > 0) {
                            if (meta.rload_3 < meta.min_rload) {
                                apply(tab_update_min_rnode3);
                            }
                            if (meta.n_replicas - 3 > 0) {
                                if (meta.rload_4 < meta.min_rload) {
                                    apply(tab_update_min_rnode4);
                                }
                            }
                        }
                    }
                    // Extend the replication set if all replicas are overloaded
                    if (meta.n_replicas < MAX_REPLICAS and
                            meta.min_rload > (meta.total_node_load >> AVG_LOAD_SHIFT)) {
                        apply(tab_extend_rset);
                    }
                }
            }
        }

        // Update node load and total load
        apply(tab_read_node_load);
        if (pegasus.op == OP_REP) {
            if (pegasus.load > 0 and meta.total_node_load > 0) {
                apply(tab_update_node_load);
                apply(tab_update_total_node_load);
            }
        } else {
            if (pegasus.load + 1 > pegasus.load and
                    meta.total_node_load + 1 > meta.total_node_load) {
                apply(tab_update_node_load);
                apply(tab_update_total_node_load);
            }
        }
        // Update minimum node load
        if (pegasus.load < meta.min_node_load) {
            apply(tab_update_min_node_load_from_hdr);
        } else if (pegasus.node == meta.min_node and pegasus.load > meta.min_node_load) {
            // The previous minimum-loaded node potentially is no longer the min node,
            // search for the new min
            apply(tab_check_min_node_load0);
            apply(tab_check_min_node_load1);
            if (meta.tmp_load < meta.min_node_load) {
                apply(tab_update_min_node_load1);
            }
            apply(tab_check_min_node_load2);
            if (meta.tmp_load < meta.min_node_load) {
                apply(tab_update_min_node_load2);
            }
            apply(tab_check_min_node_load3);
            if (meta.tmp_load < meta.min_node_load) {
                apply(tab_update_min_node_load3);
            }
            apply(tab_update_min_node_load_from_meta);
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
