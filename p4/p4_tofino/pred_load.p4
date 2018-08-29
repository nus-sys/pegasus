#include <tofino/intrinsic_metadata.p4>
#include "tofino/stateful_alu_blackbox.p4"
#include <tofino/constants.p4>

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

#define ETHERTYPE_IPV4  0x800
#define PROTO_UDP       0x11

#define PEGASUS_ID      0x5047
#define HASH_MASK       0x7 // Max 8 nodes
#define MAX_REPLICAS    0x4

#define OP_GET          0x0
#define OP_PUT          0x1
#define OP_DEL          0x2
#define OP_REP_R        0x3
#define OP_REP_W        0x4
#define OP_MGR          0x5
#define OP_MGR_REQ      0x6
#define OP_MGR_ACK      0x7
#define OP_DEC          0xF

#define RNODE_NONE      0x7F
#define RKEY_NONE       0x7F

#define OVERLOAD        0xA
#define RW_DEC          0x8
#define RW_MAX          0x30

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
        ver : 32;
        debug_node : 8;
        debug_load : 16;
    }
}

header pegasus_t pegasus;

header_type metadata_t {
    fields {
        rkey_size : 8;
        rset_size : 8;
        rkey_index : 8;
        probe_rkey_index : 8;
        probe_rset_index : 8;
        node : 8;
        load : 16;
        overload : 1;
        ver_matched : 1;
        read_heavy : 1;
    }
}

metadata metadata_t meta;

/*************************************************************************
*********************** STATEFUL MEMORY  *********************************
*************************************************************************/

/*
   Current number of outstanding requests (queue length)
   at each node
 */
register reg_queue_len {
    width: 16;
    instance_count: 32;
}
/*
   Global least loaded node
 */
register reg_min_node {
    width: 64;
    instance_count: 1;
}
/*
   Least loaded node for each rkey
 */
register reg_rkey_min_node {
    width: 64;
    instance_count: 32;
}
/*
   rkey version number
*/
register reg_rkey_ver_next {
    width: 32;
    instance_count: 32;
}
register reg_rkey_ver_curr {
    width: 32;
    instance_count: 32;
}
/*
   rkey replication set
 */
register reg_rkey_size {
    width: 8;
    instance_count: 1;
}
register reg_rset_size {
    width: 8;
    instance_count: 32;
}
register reg_rset_1 {
    width: 8;
    instance_count: 32;
}
register reg_rset_2 {
    width: 8;
    instance_count: 32;
}
register reg_rset_3 {
    width: 8;
    instance_count: 32;
}
register reg_rset_4 {
    width: 8;
    instance_count: 32;
}
/*
   Probe rkey counter
*/
register reg_probe_rkey_counter {
    width: 8;
    instance_count: 1;
}
/*
   Probe rset counter
 */
register reg_probe_rset_counter {
    width: 8;
    instance_count: 32;
}
/*
   Read/Write ratio counter
 */
register reg_rw_counter {
    width: 8;
    instance_count: 32;
}
/*************************************************************************
*********************** RESUBMIT  ****************************************
*************************************************************************/
field_list resubmit_fields {
    meta.rkey_index;
    meta.probe_rkey_index;
    meta.node;
    meta.load;
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
   drop
 */
action do_drop() {
    _drop();
}

table tab_do_drop {
    actions {
        do_drop;
    }
    default_action: do_drop;
    size: 1;
}

/*
   resubmit
 */
action do_resubmit() {
    resubmit(resubmit_fields);
}

table tab_do_resubmit {
    actions {
        do_resubmit;
    }
    default_action: do_resubmit;
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
    modify_field(meta.rkey_index, RKEY_NONE);
}

@pragma stage 0
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
   rkey version number
 */
blackbox stateful_alu sa_get_rkey_ver_next {
    reg: reg_rkey_ver_next;
    update_lo_1_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: pegasus.ver;
}
blackbox stateful_alu sa_get_rkey_ver_curr {
    reg: reg_rkey_ver_curr;
    output_value: register_lo;
    output_dst: pegasus.ver;
}
blackbox stateful_alu sa_compare_rkey_ver_curr {
    reg: reg_rkey_ver_curr;
    condition_lo: pegasus.ver == register_lo;
    output_predicate: condition_lo;
    output_value: combined_predicate;
    output_dst: meta.ver_matched;
}
blackbox stateful_alu sa_set_rkey_ver_curr {
    reg: reg_rkey_ver_curr;
    condition_lo: pegasus.ver > register_lo;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: pegasus.ver;
    output_predicate: condition_lo;
    output_value: combined_predicate;
    output_dst: meta.ver_matched;
}

action get_rkey_ver_next() {
    sa_get_rkey_ver_next.execute_stateful_alu(meta.rkey_index);
}
action get_rkey_ver_curr() {
    sa_get_rkey_ver_curr.execute_stateful_alu(meta.rkey_index);
}
action compare_rkey_ver_curr() {
    sa_compare_rkey_ver_curr.execute_stateful_alu(meta.rkey_index);
}
action set_rkey_ver_curr() {
    sa_set_rkey_ver_curr.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 1
table tab_get_rkey_ver_next {
    actions {
        get_rkey_ver_next;
    }
    default_action: get_rkey_ver_next;
    size: 1;
}
@pragma stage 1
table tab_get_rkey_ver_curr {
    actions {
        get_rkey_ver_curr;
    }
    default_action: get_rkey_ver_curr;
    size: 1;
}
@pragma stage 1
table tab_compare_rkey_ver_curr_a {
    actions {
        compare_rkey_ver_curr;
    }
    default_action: compare_rkey_ver_curr;
    size: 1;
}
@pragma stage 1
table tab_compare_rkey_ver_curr_b {
    actions {
        compare_rkey_ver_curr;
    }
    default_action: compare_rkey_ver_curr;
    size: 1;
}
@pragma stage 1
table tab_set_rkey_ver_curr_a {
    actions {
        set_rkey_ver_curr;
    }
    default_action: set_rkey_ver_curr;
    size: 1;
}
@pragma stage 1
table tab_set_rkey_ver_curr_b {
    actions {
        set_rkey_ver_curr;
    }
    default_action: set_rkey_ver_curr;
    size: 1;
}

/*
   get/inc/dec queue len
 */
blackbox stateful_alu sa_get_queue_len {
    reg: reg_queue_len;
    output_value: register_lo;
    output_dst: meta.load;
}
blackbox stateful_alu sa_inc_queue_len {
    reg: reg_queue_len;
    update_lo_1_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: meta.load;
}
blackbox stateful_alu sa_dec_queue_len {
    reg: reg_queue_len;
    condition_lo: register_lo > meta.load;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: register_lo - meta.load;
}

action get_queue_len() {
    sa_get_queue_len.execute_stateful_alu(meta.node);
}
action inc_queue_len() {
    sa_inc_queue_len.execute_stateful_alu(meta.node);
}
action dec_queue_len() {
    sa_dec_queue_len.execute_stateful_alu(meta.node);
    add(meta.node, meta.node, 1);
}

@pragma stage 6
table tab_get_queue_len {
    actions {
        get_queue_len;
    }
    default_action: get_queue_len;
    size: 1;
}
@pragma stage 6
table tab_inc_queue_len {
    actions {
        inc_queue_len;
    }
    default_action: inc_queue_len;
    size: 1;
}
@pragma stage 6
table tab_dec_queue_len_a {
    actions {
        dec_queue_len;
    }
    default_action: dec_queue_len;
    size: 1;
}
@pragma stage 6
table tab_dec_queue_len_b {
    actions {
        dec_queue_len;
    }
    default_action: dec_queue_len;
    size: 1;
}

/*
   dummies
 */
blackbox stateful_alu sa_dummy {
    reg: reg_rkey_ver_curr;
}

action dummy() {
    sa_dummy.execute_stateful_alu(0);
}

@pragma stage 1
table tab_dummy {
    actions {
        dummy;
    }
    default_action: dummy;
    size: 1;
}

/*
   get/set/compare min node
 */
blackbox stateful_alu sa_get_min_node {
    reg: reg_min_node;
    output_value: register_hi;
    output_dst: meta.node;
}
blackbox stateful_alu sa_compare_min_node {
    reg: reg_min_node;
    condition_lo: meta.load < register_lo;
    condition_hi: meta.node == register_hi;
    update_lo_1_predicate: condition_lo or condition_hi;
    update_lo_1_value: meta.load;
    update_hi_1_predicate: condition_lo;
    update_hi_1_value: meta.node;
    output_value: register_hi;
    output_dst: pegasus.node; // for MGR
}

action get_min_node() {
    sa_get_min_node.execute_stateful_alu(0);
}
action compare_min_node() {
    sa_compare_min_node.execute_stateful_alu(0);
}

@pragma stage 3
table tab_get_min_node {
    actions {
        get_min_node;
    }
    default_action: get_min_node;
    size: 1;
}
@pragma stage 3
table tab_compare_min_node {
    actions {
        compare_min_node;
    }
    default_action: compare_min_node;
    size: 1;
}

/*
   get/set rkey min node
 */
blackbox stateful_alu sa_get_rkey_min_node {
    reg: reg_rkey_min_node;
    output_value: register_hi;
    output_dst: meta.node;
}
blackbox stateful_alu sa_set_rkey_min_node {
    reg: reg_rkey_min_node;
     // okay to set 0...will be updated by subsequent packets
    update_lo_1_value: 0;
    update_hi_1_value: meta.node;
}
blackbox stateful_alu sa_compare_rkey_min_node {
    reg: reg_rkey_min_node;
    condition_lo: meta.load < register_lo;
    condition_hi: meta.node == register_hi;
    update_lo_1_predicate: condition_lo or condition_hi;
    update_lo_1_value: meta.load;
    update_hi_1_predicate: condition_lo or condition_hi;
    update_hi_1_value: meta.node;
}
blackbox stateful_alu sa_check_overload_rkey_min_node {
    reg: reg_rkey_min_node;
    condition_lo: meta.load > OVERLOAD;
    condition_hi: meta.node == register_hi;
    update_lo_1_predicate: condition_hi;
    update_lo_1_value: meta.load;
    output_predicate: condition_lo and condition_hi;
    output_value: combined_predicate;
    output_dst: meta.overload;
}

action get_rkey_min_node() {
    sa_get_rkey_min_node.execute_stateful_alu(meta.rkey_index);
}
action set_rkey_min_node() {
    sa_set_rkey_min_node.execute_stateful_alu(meta.rkey_index);
}
action compare_rkey_min_node() {
    sa_compare_rkey_min_node.execute_stateful_alu(meta.probe_rkey_index);
}
action check_overload_rkey_min_node() {
    sa_check_overload_rkey_min_node.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 3
table tab_get_rkey_min_node {
    actions {
        get_rkey_min_node;
    }
    default_action: get_rkey_min_node;
    size: 1;
}
@pragma stage 3
table tab_set_rkey_min_node_a {
    actions {
        set_rkey_min_node;
    }
    default_action: set_rkey_min_node;
    size: 1;
}
@pragma stage 3
table tab_set_rkey_min_node_b {
    actions {
        set_rkey_min_node;
    }
    default_action: set_rkey_min_node;
    size: 1;
}
@pragma stage 3
table tab_compare_rkey_min_node {
    actions {
        compare_rkey_min_node;
    }
    default_action: compare_rkey_min_node;
    size: 1;
}
@pragma stage 3
table tab_check_overload_rkey_min_node {
    actions {
        check_overload_rkey_min_node;
    }
    default_action: check_overload_rkey_min_node;
    size: 1;
}

/*
   get/set rnode (1-4)
*/
blackbox stateful_alu sa_get_rnode_1 {
    reg: reg_rset_1;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_set_rnode_1 {
    reg: reg_rset_1;
    update_lo_1_value: meta.node;
}
blackbox stateful_alu sa_get_rnode_2 {
    reg: reg_rset_2;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_set_rnode_2 {
    reg: reg_rset_2;
    update_lo_1_value: RNODE_NONE;
}
blackbox stateful_alu sa_install_rnode_2 {
    reg: reg_rset_2;
    update_lo_1_value: meta.node;
}
blackbox stateful_alu sa_get_rnode_3 {
    reg: reg_rset_3;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_set_rnode_3 {
    reg: reg_rset_3;
    update_lo_1_value: RNODE_NONE;
}
blackbox stateful_alu sa_install_rnode_3 {
    reg: reg_rset_3;
    update_lo_1_value: meta.node;
}
blackbox stateful_alu sa_get_rnode_4 {
    reg: reg_rset_4;
    output_value: register_lo;
    output_dst: meta.node;
}
blackbox stateful_alu sa_set_rnode_4 {
    reg: reg_rset_4;
    update_lo_1_value: RNODE_NONE;
}
blackbox stateful_alu sa_install_rnode_4 {
    reg: reg_rset_4;
    update_lo_1_value: meta.node;
}

action get_rnode_1() {
    sa_get_rnode_1.execute_stateful_alu(meta.probe_rkey_index);
}
action set_rnode_1() {
    sa_set_rnode_1.execute_stateful_alu(meta.rkey_index);
}
action get_rnode_2() {
    sa_get_rnode_2.execute_stateful_alu(meta.probe_rkey_index);
}
action set_rnode_2() {
    sa_set_rnode_2.execute_stateful_alu(meta.rkey_index);
}
action install_rnode_2() {
    sa_install_rnode_2.execute_stateful_alu(meta.rkey_index);
}
action get_rnode_3() {
    sa_get_rnode_3.execute_stateful_alu(meta.probe_rkey_index);
}
action set_rnode_3() {
    sa_set_rnode_3.execute_stateful_alu(meta.rkey_index);
}
action install_rnode_3() {
    sa_install_rnode_3.execute_stateful_alu(meta.rkey_index);
}
action get_rnode_4() {
    sa_get_rnode_4.execute_stateful_alu(meta.probe_rkey_index);
}
action set_rnode_4() {
    sa_set_rnode_4.execute_stateful_alu(meta.rkey_index);
}
action install_rnode_4() {
    sa_install_rnode_4.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 4
table tab_get_rnode_1 {
    actions {
        get_rnode_1;
    }
    default_action: get_rnode_1;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_1_a {
    actions {
        set_rnode_1;
    }
    default_action: set_rnode_1;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_1_b {
    actions {
        set_rnode_1;
    }
    default_action: set_rnode_1;
    size: 1;
}
@pragma stage 4
table tab_get_rnode_2 {
    actions {
        get_rnode_2;
    }
    default_action: get_rnode_2;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_2_a {
    actions {
        set_rnode_2;
    }
    default_action: set_rnode_2;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_2_b {
    actions {
        set_rnode_2;
    }
    default_action: set_rnode_2;
    size: 1;
}
@pragma stage 4
table tab_install_rnode_2_a {
    actions {
        install_rnode_2;
    }
    default_action: install_rnode_2;
    size: 1;
}
@pragma stage 4
table tab_install_rnode_2_b {
    actions {
        install_rnode_2;
    }
    default_action: install_rnode_2;
    size: 1;
}
@pragma stage 4
table tab_get_rnode_3 {
    actions {
        get_rnode_3;
    }
    default_action: get_rnode_3;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_3_a {
    actions {
        set_rnode_3;
    }
    default_action: set_rnode_3;
    size: 1;
}
@pragma stage 4
table tab_set_rnode_3_b {
    actions {
        set_rnode_3;
    }
    default_action: set_rnode_3;
    size: 1;
}
@pragma stage 4
table tab_install_rnode_3_a {
    actions {
        install_rnode_3;
    }
    default_action: install_rnode_3;
    size: 1;
}
@pragma stage 4
table tab_install_rnode_3_b {
    actions {
        install_rnode_3;
    }
    default_action: install_rnode_3;
    size: 1;
}
@pragma stage 5
table tab_get_rnode_4 {
    actions {
        get_rnode_4;
    }
    default_action: get_rnode_4;
    size: 1;
}
@pragma stage 5
table tab_set_rnode_4_a {
    actions {
        set_rnode_4;
    }
    default_action: set_rnode_4;
    size: 1;
}
@pragma stage 5
table tab_set_rnode_4_b {
    actions {
        set_rnode_4;
    }
    default_action: set_rnode_4;
    size: 1;
}
@pragma stage 5
table tab_install_rnode_4_a {
    actions {
        install_rnode_4;
    }
    default_action: install_rnode_4;
    size: 1;
}
@pragma stage 5
table tab_install_rnode_4_b {
    actions {
        install_rnode_4;
    }
    default_action: install_rnode_4;
    size: 1;
}

/*
   get/set rkey/rset size
*/
blackbox stateful_alu sa_get_rkey_size {
    reg: reg_rkey_size;
    output_value: register_lo;
    output_dst: meta.rkey_size;
}
blackbox stateful_alu sa_get_rset_size {
    reg: reg_rset_size;
    output_value: register_lo;
    output_dst: meta.rset_size;
}
blackbox stateful_alu sa_set_rset_size {
    reg: reg_rset_size;
    update_lo_1_value: 1;
}
blackbox stateful_alu sa_inc_rset_size {
    reg: reg_rset_size;
    condition_lo: register_lo < MAX_REPLICAS;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: register_lo + 1;
    output_value: register_lo;
    output_dst: meta.rset_size;
}

action get_rkey_size() {
    sa_get_rkey_size.execute_stateful_alu(0);
}
action get_rset_size_a() {
    // for probing
    sa_get_rset_size.execute_stateful_alu(meta.probe_rkey_index);
}
action get_rset_size_b() {
    // for checking migration
    sa_get_rset_size.execute_stateful_alu(meta.rkey_index);
}
action set_rset_size() {
    sa_set_rset_size.execute_stateful_alu(meta.rkey_index);
}
action inc_rset_size() {
    sa_inc_rset_size.execute_stateful_alu(meta.rkey_index);
}

@pragma stage 0
table tab_get_rkey_size {
    actions {
        get_rkey_size;
    }
    default_action: get_rkey_size;
    size: 1;
}
@pragma stage 2
table tab_get_rset_size_a {
    // for probing
    actions {
        get_rset_size_a;
    }
    default_action: get_rset_size_a;
    size: 1;
}
@pragma stage 2
table tab_get_rset_size_b {
    // for checking migration
    actions {
        get_rset_size_b;
    }
    default_action: get_rset_size_b;
    size: 1;
}
@pragma stage 2
table tab_set_rset_size_a {
    actions {
        set_rset_size;
    }
    default_action: set_rset_size;
    size: 1;
}
@pragma stage 2
table tab_set_rset_size_b {
    actions {
        set_rset_size;
    }
    default_action: set_rset_size;
    size: 1;
}
@pragma stage 2
table tab_inc_rset_size_a {
    actions {
        inc_rset_size;
    }
    default_action: inc_rset_size;
    size: 1;
}
@pragma stage 2
table tab_inc_rset_size_b {
    actions {
        inc_rset_size;
    }
    default_action: inc_rset_size;
    size: 1;
}

/*
   get probe rkey/rset counter
 */
blackbox stateful_alu sa_get_probe_rkey_counter {
    reg: reg_probe_rkey_counter;
    condition_lo: register_lo + 1 >= meta.rkey_size;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: 0;
    update_lo_2_predicate: not condition_lo;
    update_lo_2_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: meta.probe_rkey_index;
}
blackbox stateful_alu sa_get_probe_rset_counter {
    reg: reg_probe_rset_counter;
    condition_lo: register_lo + 1 >= meta.rset_size;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: 0;
    update_lo_2_predicate: not condition_lo;
    update_lo_2_value: register_lo + 1;
    output_value: alu_lo;
    output_dst: meta.probe_rset_index;
}

action get_probe_rkey_counter() {
    sa_get_probe_rkey_counter.execute_stateful_alu(0);
}
action get_probe_rset_counter() {
    sa_get_probe_rset_counter.execute_stateful_alu(meta.probe_rkey_index);
}

@pragma stage 1
table tab_get_probe_rkey_counter {
    actions {
        get_probe_rkey_counter;
    }
    default_action: get_probe_rkey_counter;
    size: 1;
}
@pragma stag 3
table tab_get_probe_rset_counter {
    actions {
        get_probe_rset_counter;
    }
    default_action: get_probe_rset_counter;
    size: 1;
}

/*
   set rkey/probe none
 */
action set_rkey_none() {
    modify_field(meta.rkey_index, RKEY_NONE);
}
action set_probe_none() {
    modify_field(meta.probe_rkey_index, RKEY_NONE);
}

table tab_set_rkey_none {
    actions {
        set_rkey_none;
    }
    default_action: set_rkey_none;
    size: 1;
}
table tab_set_probe_none {
    actions {
        set_probe_none;
    }
    default_action: set_probe_none;
    size: 1;
}

/*
   rw counter
 */
blackbox stateful_alu sa_get_rw_counter {
    reg: reg_rw_counter;
    condition_lo: register_lo >= RW_DEC;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: register_lo - RW_DEC;
    output_predicate: condition_lo;
    output_value: combined_predicate;
    output_dst: meta.read_heavy;
}
blackbox stateful_alu sa_inc_rw_counter {
    reg: reg_rw_counter;
    condition_lo: register_lo < RW_MAX;
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: register_lo + 1;
}

action get_rw_counter() {
    sa_get_rw_counter.execute_stateful_alu(meta.rkey_index);
}
action inc_rw_counter() {
    sa_inc_rw_counter.execute_stateful_alu(meta.rkey_index);
}

table tab_get_rw_counter {
    actions {
        get_rw_counter;
    }
    default_action: get_rw_counter;
    size: 1;
}
table tab_inc_rw_counter {
    actions {
        inc_rw_counter;
    }
    default_action: inc_rw_counter;
    size: 1;
}

/*
   rkey migration
 */
action rkey_migration() {
    modify_field(pegasus.op, OP_MGR);
}

table tab_rkey_migration {
    actions {
        rkey_migration;
    }
    default_action: rkey_migration;
    size: 1;
}

/*
   copy header
 */
action copy_pegasus_header() {
    modify_field(meta.node, pegasus.node);
    modify_field(meta.load, pegasus.load);
}

table tab_copy_pegasus_header_a {
    actions {
        copy_pegasus_header;
    }
    default_action: copy_pegasus_header;
    size: 1;
}
table tab_copy_pegasus_header_b {
    actions {
        copy_pegasus_header;
    }
    default_action: copy_pegasus_header;
    size: 1;
}
table tab_copy_pegasus_header_c {
    actions {
        copy_pegasus_header;
    }
    default_action: copy_pegasus_header;
    size: 1;
}

/*
   write_num_replicas
 */
action write_local_replicas() {
    // Currently just replicate to all MAX_REPLICAS local nodes
    modify_field(pegasus.debug_node, MAX_REPLICAS);
}

table tab_write_local_replicas {
    actions {
        write_local_replicas;
    }
    default_action : write_local_replicas;
    size : 1;
}

/*
   debug
*/
action debug() {
    modify_field(pegasus.debug_node, meta.node);
    modify_field(pegasus.debug_load, meta.load);
}

table tab_debug {
    actions {
        debug;
    }
    default_action: debug;
    size: 1;
}

control process_reply {
    apply(tab_get_rkey_size);
    if (meta.rkey_index != RKEY_NONE and pegasus.op == OP_REP_W) {
        // Check if the replicated write reply has a higher version number
        apply(tab_copy_pegasus_header_a);
        apply(tab_set_rkey_ver_curr_a);
        if (meta.ver_matched != 0) {
            apply(tab_set_rset_size_a);
            apply(tab_set_rkey_min_node_a);
            apply(tab_set_rnode_1_a);
            apply(tab_set_rnode_2_a);
            apply(tab_set_rnode_3_a);
            apply(tab_set_rnode_4_a);
            apply(tab_l2_forward);
        } else {
            apply(tab_do_resubmit);
        }
    } else {
        apply(tab_set_rkey_none);
        // Probe rkey min node
        if (meta.rkey_size != 0) {
            apply(tab_get_probe_rkey_counter);
            apply(tab_get_rset_size_a);
            apply(tab_get_probe_rset_counter);
            if (meta.probe_rset_index == 0) {
                apply(tab_get_rnode_1);
            } else if (meta.probe_rset_index == 1) {
                apply(tab_get_rnode_2);
            } else if (meta.probe_rset_index == 2) {
                apply(tab_get_rnode_3);
            } else if (meta.probe_rset_index == 3) {
                apply(tab_get_rnode_4);
            }
            if (meta.node != RNODE_NONE) {
                apply(tab_get_queue_len);
            }
        } else {
            // Nothing to probe
            apply(tab_set_probe_none);
        }
        apply(tab_do_resubmit);
    }
}

control process_dec {
    apply(tab_copy_pegasus_header_b);
    apply(tab_dec_queue_len_a);
    apply(tab_do_resubmit);
}

control process_mgr {
    // Should never receive MGR message
    apply(tab_do_drop);
}

control process_mgr_req {
    apply(tab_l2_forward);
}

control process_mgr_ack {
    apply(tab_copy_pegasus_header_c);
    if (meta.rkey_index != RKEY_NONE) {
        apply(tab_set_rkey_ver_curr_b);
        if (meta.ver_matched != 0) {
            apply(tab_set_rset_size_b);
            apply(tab_set_rkey_min_node_b);
            apply(tab_set_rnode_1_b);
            apply(tab_set_rnode_2_b);
            apply(tab_set_rnode_3_b);
            apply(tab_set_rnode_4_b);
            apply(tab_do_drop);
        } else {
            apply(tab_do_resubmit);
        }
    } else {
        apply(tab_do_drop);
    }
}

control process_request {
    if (meta.rkey_index != RKEY_NONE) {
        if (pegasus.op == OP_GET) {
            process_replicated_read();
        } else {
            process_replicated_write();
        }
    }
    apply(tab_inc_queue_len);
    apply(tab_do_resubmit);
}

control process_replicated_read {
    apply(tab_get_rkey_min_node);
}

control process_replicated_write {
    apply(tab_get_min_node);
}

control process_resubmit_reply {
    if (meta.rkey_index != RKEY_NONE) {
        if (pegasus.op == OP_REP_W) {
            // meta.node already copied in the first pass
            // another replica has received the latest write, install
            // it in the replication set
            apply(tab_compare_rkey_ver_curr_b);
            if (meta.ver_matched != 0) {
                apply(tab_inc_rset_size_b);
                if (meta.rset_size == 1) {
                    apply(tab_install_rnode_2_b);
                } else if (meta.rset_size == 2) {
                    apply(tab_install_rnode_3_b);
                } else if (meta.rset_size == 3) {
                    apply(tab_install_rnode_4_b);
                }
            }
        }
    } else {
        if (meta.probe_rkey_index != RKEY_NONE) {
            if (meta.node != RNODE_NONE) {
                apply(tab_compare_rkey_min_node);
            }
        }
    }
    apply(tab_l2_forward);
}

control process_resubmit_dec {
    apply(tab_dummy);
    apply(tab_dec_queue_len_b);
    apply(tab_do_drop);
}

control process_resubmit_mgr_ack {
    if (meta.rkey_index != RKEY_NONE) {
        apply(tab_compare_rkey_ver_curr_a);
        if (meta.ver_matched != 0) {
            apply(tab_inc_rset_size_a);
            if (meta.rset_size == 1) {
                apply(tab_install_rnode_2_a);
            } else if (meta.rset_size == 2) {
                apply(tab_install_rnode_3_a);
            } else if (meta.rset_size == 3) {
                apply(tab_install_rnode_4_a);
            }
        }
    }
    apply(tab_do_drop);
}

control process_resubmit_request {
    if (meta.rkey_index != RKEY_NONE) {
        if (pegasus.op == OP_GET) {
            apply(tab_get_rkey_ver_curr);
            apply(tab_inc_rw_counter);
            apply(tab_get_rset_size_b);
            if (meta.rset_size != MAX_REPLICAS) {
                apply(tab_check_overload_rkey_min_node);
            }
        } else {
            apply(tab_get_rkey_ver_next);
            apply(tab_get_rw_counter);
            if (meta.read_heavy != 0) {
                apply(tab_write_local_replicas);
            }
        }
    }
    apply(tab_compare_min_node);
    if (meta.overload != 0) {
        apply(tab_rkey_migration);
    }
    //apply(tab_debug);
    apply(tab_node_forward);
}

control ingress {
    if (0 == ig_intr_md.resubmit_flag) {
        if (valid(pegasus)) {
            if (pegasus.op == OP_DEC) {
                process_dec();
            } else if (pegasus.op == OP_MGR) {
                process_mgr();
            } else if (pegasus.op == OP_MGR_REQ) {
                process_mgr_req();
            } else {
                apply(tab_replicated_keys);
                if (pegasus.op == OP_REP_R or pegasus.op == OP_REP_W) {
                    process_reply();
                } else if (pegasus.op == OP_MGR_ACK) {
                    process_mgr_ack();
                } else {
                    process_request();
                }
            }
        } else {
            apply(tab_l2_forward);
        }
    } else {
        if (valid(pegasus)) {
            if (pegasus.op == OP_REP_R or pegasus.op == OP_REP_W) {
                process_resubmit_reply();
            } else if (pegasus.op == OP_DEC) {
                process_resubmit_dec();
            } else if (pegasus.op == OP_MGR_ACK) {
                process_resubmit_mgr_ack();
            } else {
                process_resubmit_request();
            }
        }
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control egress {
}
