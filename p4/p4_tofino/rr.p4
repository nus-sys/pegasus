#include <tofino/intrinsic_metadata.p4>
#include "tofino/stateful_alu_blackbox.p4"
#include <tofino/constants.p4>

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

#define ETHERTYPE_IPV4  0x800
#define PROTO_UDP       0x11
#define PEGASUS_ID      0x5047
#define HASH_MASK       0x3 // Max 4 nodes
#define NNODES          0x4
#define OP_GET          0x0
#define OP_PUT          0x1
#define OP_DEL          0x2
#define OP_REP          0x3
#define RNODE_NONE      0x7F

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
        // Replica nodes
        rkey_index : 32;
        rkey_counter : 16;
        nrnodes : 16;
        rnode_index: 16;
        // Temporary variables
        node : 8;
        load : 16;
    }
}

metadata metadata_t meta;

/*************************************************************************
*********************** STATEFUL MEMORY  *********************************
*************************************************************************/
register reg_node_load {
    width: 16;
    instance_count: 32;
}
register reg_rkey_read_counter {
    width: 16;
    instance_count: 32;
}
register reg_rkey_write_counter {
    width: 16;
    instance_count: 32;
}
register reg_nrnodes {
    width: 16;
    instance_count: 32;
}
register reg_rnode_1 {
    width: 8;
    instance_count: 32;
}
register reg_rnode_2 {
    width: 8;
    instance_count: 32;
}
register reg_rnode_3 {
    width: 8;
    instance_count: 32;
}
register reg_rnode_4 {
    width: 8;
    instance_count: 32;
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

/*
   L2 forward
*/
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
    size: 1024;
}

/*
   node forward
*/
action node_forward(mac_addr, ip_addr, udp_addr, port) {
    modify_field(ethernet.dstAddr, mac_addr);
    modify_field(ipv4.dstAddr, ip_addr);
    modify_field(udp.dstPort, udp_addr);
    modify_field(ig_intr_md_for_tm.ucast_egress_port, port);
}

table tab_node_forward {
    reads {
        meta.node: exact;
    }
    actions {
        node_forward;
        _drop;
    }
    size: 32;
}

/*
   update node load
*/
blackbox stateful_alu sa_update_node_load {
    reg: reg_node_load;
    update_lo_1_value: pegasus.load;
}

action update_node_load() {
    sa_update_node_load.execute_stateful_alu(pegasus.node);
}

table tab_update_node_load {
    actions {
        update_node_load;
    }
    default_action: update_node_load;
    size: 1;
}

/*
   replicated keys
*/
action lookup_rkey(rkey_index) {
    modify_field(meta.rkey_index, rkey_index);
}

action set_default_dst_node() {
    bit_and(meta.node, pegasus.keyhash, HASH_MASK);
}

table tab_replicated_keys {
    reads {
        pegasus.keyhash: exact;
    }
    actions {
        lookup_rkey;
        set_default_dst_node;
    }
    default_action: set_default_dst_node;
    size: 32;
}

/*
   access rkey read/write counter
 */
blackbox stateful_alu sa_access_rkey_read_counter {
    reg: reg_rkey_read_counter;
    condition_lo: register_lo == meta.nrnodes;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: 0;
    update_lo_2_predicate: not condition_lo;
    update_lo_2_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: meta.rnode_index;
}

action access_rkey_read_counter() {
    sa_access_rkey_read_counter.execute_stateful_alu(meta.rkey_index);
}

table tab_access_rkey_read_counter {
    actions {
        access_rkey_read_counter;
    }
    default_action: access_rkey_read_counter;
    size: 1;
}

blackbox stateful_alu sa_access_rkey_write_counter {
    reg: reg_rkey_write_counter;
    condition_lo: register_lo == NNODES - 1;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: 0;
    update_lo_2_predicate: not condition_lo;
    update_lo_2_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: meta.node;
}

action access_rkey_write_counter() {
    sa_access_rkey_write_counter.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 2
table tab_access_rkey_write_counter {
    actions {
        access_rkey_write_counter;
    }
    default_action: access_rkey_write_counter;
    size: 1;
}

/*
   read nrnodes
 */
blackbox stateful_alu sa_read_nrnodes {
    reg: reg_nrnodes;
    output_value: register_lo;
    output_dst: meta.nrnodes;
}

action read_nrnodes() {
    sa_read_nrnodes.execute_stateful_alu(meta.rkey_index);
}

table tab_read_nrnodes {
    actions {
        read_nrnodes;
    }
    default_action: read_nrnodes;
    size: 1;
}

/*
   update nrnodes
 */
blackbox stateful_alu sa_update_nrnodes {
    reg: reg_nrnodes;
    update_lo_1_value: 0;
}

action update_nrnodes() {
    sa_update_nrnodes.execute_stateful_alu(meta.rkey_index);
}

table tab_update_nrnodes {
    actions {
        update_nrnodes;
    }
    default_action: update_nrnodes;
    size: 1;
}

/*
   read rnode
 */
blackbox stateful_alu sa_read_rnode_1 {
    reg: reg_rnode_1;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_read_rnode_2 {
    reg: reg_rnode_2;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_read_rnode_3 {
    reg: reg_rnode_3;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_read_rnode_4 {
    reg: reg_rnode_4;
    output_value: register_lo;
    output_dst: meta.node;
}

action read_rnode_1() {
    sa_read_rnode_1.execute_stateful_alu(meta.rkey_index);
}
action read_rnode_2() {
    sa_read_rnode_2.execute_stateful_alu(meta.rkey_index);
}
action read_rnode_3() {
    sa_read_rnode_3.execute_stateful_alu(meta.rkey_index);
}
action read_rnode_4() {
    sa_read_rnode_4.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 3
table tab_read_rnode_1 {
    actions {
        read_rnode_1;
    }
    default_action: read_rnode_1;
    size: 1;
}
@pragma stage 3
table tab_read_rnode_2 {
    actions {
        read_rnode_2;
    }
    default_action: read_rnode_2;
    size: 1;
}
@pragma stage 3
table tab_read_rnode_3 {
    actions {
        read_rnode_3;
    }
    default_action: read_rnode_3;
    size: 1;
}
@pragma stage 3
table tab_read_rnode_4 {
    actions {
        read_rnode_4;
    }
    default_action: read_rnode_4;
    size: 1;
}

/*
   update rnode (1-4)
*/
blackbox stateful_alu sa_update_rnode_1 {
    reg: reg_rnode_1;
    update_lo_1_value: meta.node;
}
blackbox stateful_alu sa_update_rnode_2 {
    reg: reg_rnode_2;
    update_lo_1_value: RNODE_NONE;
}
blackbox stateful_alu sa_update_rnode_3 {
    reg: reg_rnode_3;
    update_lo_1_value: RNODE_NONE;
}
blackbox stateful_alu sa_update_rnode_4 {
    reg: reg_rnode_4;
    update_lo_1_value: RNODE_NONE;
}

action update_rnode_1() {
    sa_update_rnode_1.execute_stateful_alu(meta.rkey_index);
}
action update_rnode_2() {
    sa_update_rnode_2.execute_stateful_alu(meta.rkey_index);
}
action update_rnode_3() {
    sa_update_rnode_3.execute_stateful_alu(meta.rkey_index);
}
action update_rnode_4() {
    sa_update_rnode_4.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 3
table tab_update_rnode_1 {
    actions {
        update_rnode_1;
    }
    default_action: update_rnode_1;
    size: 1;
}
@pragma stage 3
table tab_update_rnode_2 {
    actions {
        update_rnode_2;
    }
    default_action: update_rnode_2;
    size: 1;
}
@pragma stage 3
table tab_update_rnode_3 {
    actions {
        update_rnode_3;
    }
    default_action: update_rnode_3;
    size: 1;
}
@pragma stage 3
table tab_update_rnode_4 {
    actions {
        update_rnode_4;
    }
    default_action: update_rnode_4;
    size: 1;
}

/*
   debug
*/
action debug() {
    modify_field(pegasus.node, meta.node);
    modify_field(pegasus.load, meta.load);
}

table tab_debug {
    actions {
        debug;
    }
    default_action: debug;
    size: 1;
}

control process_pegasus_reply {
    apply(tab_update_node_load);
    apply(tab_l2_forward);
}

control process_pegasus_request {
    apply(tab_replicated_keys) {
        hit {
            if (pegasus.op == OP_GET) {
                process_replicated_read();
            } else {
                process_replicated_write();
            }
        }
    }
    apply(tab_node_forward);
    apply(tab_debug);
}

control process_replicated_read {
    apply(tab_read_nrnodes);
    apply(tab_access_rkey_read_counter);
    if (meta.rnode_index == 0) {
        apply(tab_read_rnode_1);
    } else if (meta.rnode_index == 1) {
        apply(tab_read_rnode_2);
    } else if (meta.rnode_index == 2) {
        apply(tab_read_rnode_3);
    } else if (meta.rnode_index == 3) {
        apply(tab_read_rnode_4);
    }
}

control process_replicated_write {
    apply(tab_update_nrnodes);
    apply(tab_access_rkey_write_counter);
    apply(tab_update_rnode_1);
    apply(tab_update_rnode_2);
    apply(tab_update_rnode_3);
    apply(tab_update_rnode_4);
}

control ingress {
    if (valid(pegasus)) {
        if (pegasus.op == OP_REP) {
            process_pegasus_reply();
        } else {
            process_pegasus_request();
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
