/* -*- P4_16 -*- */
#include <core.p4>
#include <v1model.p4>

/*************************************************************************
*********************** H E A D E R S  ***********************************
*************************************************************************/

typedef bit<9>  egressSpec_t;
typedef bit<48> macAddr_t;
typedef bit<32> ip4Addr_t;
typedef bit<16> udpPort_t;
typedef bit<8>  op_t;
typedef bit<32> keyhash_t;
typedef bit<16> load_t;
typedef bit<4>  node_t;

const bit<16> TYPE_IPV4 = 0x800;
const bit<8> PROTO_UDP = 0x11;
const op_t OP_GET = 0x0;
const op_t OP_PUT = 0x1;
const op_t OP_DEL = 0x2;
const op_t OP_REP = 0x3;
const bit<16> PEGASUS_ID = 0x5047; //PG
const bit<32> HASH_MASK = 0x3; // Max 4 nodes
const load_t MIN_LOAD_INIT_VALUE = 0xFFFF;
const bit<3> MAX_REPLICAS = 0x4;
const bit<3> AVG_LOAD_SHIFT = 0x2;

header ethernet_t {
    macAddr_t dstAddr;
    macAddr_t srcAddr;
    bit<16>   etherType;
}

header ipv4_t {
    bit<4>    version;
    bit<4>    ihl;
    bit<8>    diffserv;
    bit<16>   totalLen;
    bit<16>   identification;
    bit<3>    flags;
    bit<13>   fragOffset;
    bit<8>    ttl;
    bit<8>    protocol;
    bit<16>   hdrChecksum;
    ip4Addr_t srcAddr;
    ip4Addr_t dstAddr;
}

header udp_t {
    udpPort_t   srcPort;
    udpPort_t   dstPort;
    bit<16>     len;
    bit<16>     checksum;
}

header pegasus_t {
    bit<16>     id;
    op_t        op;
    keyhash_t   keyhash;
    node_t      node;
    bit<4>      pad;
    load_t      load;
}

struct metadata {
    node_t dst_node;
    load_t nload;
    load_t total_nload;
    load_t min_nload;
    node_t min_nload_node;
    // replicated keys
    bit<32> rkey_index;
    bit<64> rkey;
    bit<1> update_rkey;
    load_t min_rload;
    bit<3> n_replicas;
    node_t node_1;
    node_t node_2;
    node_t node_3;
    node_t node_4;
}

struct headers {
    ethernet_t   ethernet;
    ipv4_t       ipv4;
    udp_t        udp;
    pegasus_t    pegasus;
}

/*************************************************************************
*********************** STATEFUL MEMORY  *******************************
*************************************************************************/
register<load_t>(1) total_nload;
register<load_t>(1) min_nload;
register<node_t>(1) min_nload_node;
register<load_t>(32) node_load;
register<bit<64>>(32) replicated_keys; // Format: keyhash + nreplicas(3) + node1(4) + node2(4) + ... (max 4 replicas)

/*************************************************************************
*********************** P A R S E R  ***********************************
*************************************************************************/

parser MyParser(packet_in packet,
                out headers hdr,
                inout metadata meta,
                inout standard_metadata_t standard_metadata) {

    state start {
	packet.extract(hdr.ethernet);
        transition select (hdr.ethernet.etherType) {
            TYPE_IPV4: parse_ipv4;
            default: accept;
	}
    }

    state parse_ipv4 {
        packet.extract(hdr.ipv4);
        transition select (hdr.ipv4.protocol) {
            PROTO_UDP: parse_udp;
            default: accept;
        }
    }

    state parse_udp {
        packet.extract(hdr.udp);
        transition select(packet.lookahead<pegasus_t>().id) {
            PEGASUS_ID : parse_pegasus;
            default: accept;
        }
    }

    state parse_pegasus {
        packet.extract(hdr.pegasus);
        transition accept;
    }
}


/*************************************************************************
************   C H E C K S U M    V E R I F I C A T I O N   *************
*************************************************************************/

control MyVerifyChecksum(inout headers hdr, inout metadata meta) {
    apply {  }
}


/*************************************************************************
**************  I N G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control MyIngress(inout headers hdr,
                  inout metadata meta,
                  inout standard_metadata_t standard_metadata) {
    action drop() {
        mark_to_drop();
    }

    action l2_forward(egressSpec_t port) {
        standard_metadata.egress_spec = port;
    }

    table tab_mac {
        key = {
            hdr.ethernet.dstAddr: exact;
        }
        actions = {
            l2_forward;
            drop;
        }
        size = 1024;
        default_action = drop();
    }

    action node_forward(macAddr_t macAddr, ip4Addr_t ip4Addr, egressSpec_t port) {
        hdr.ethernet.dstAddr = macAddr;
        hdr.ipv4.dstAddr = ip4Addr;
        standard_metadata.egress_spec = port;
    }

    table tab_node_forward {
        key = {
            meta.dst_node: exact;
        }
        actions = {
            node_forward;
            drop;
        }
        size = 32;
        default_action = drop();
    }

    action update_min_nload(load_t load, node_t node) {
        min_nload.write(0, load);
        min_nload_node.write(0, node);
    }

    action check_min_nload(node_t node) {
        load_t load;
        node_load.read(load, (bit<32>)node);
        if (load < meta.min_nload) {
            meta.min_nload = load;
            meta.min_nload_node = node;
        }
    }

    action read_nload_stats() {
        min_nload.read(meta.min_nload, 0);
        min_nload_node.read(meta.min_nload_node, 0);
        total_nload.read(meta.total_nload, 0);
    }

    action dec_node_load() {
        load_t load;
        node_load.read(load, (bit<32>)meta.dst_node);
        if (load > 0) {
            load = load - 1;
        }
        node_load.write((bit<32>)meta.dst_node, load);
        meta.nload = load;

        total_nload.read(load, 0);
        if (load > 0) {
            load = load - 1;
        }
        total_nload.write(0, load);
    }

    action inc_node_load() {
        load_t load;
        node_load.read(load, (bit<32>)meta.dst_node);
        if (load + 1 > load) {
            // Prevent overflow
            load = load + 1;
        }
        node_load.write((bit<32>)meta.dst_node, load);
        meta.nload = load;

        total_nload.read(load, 0);
        if (load + 1 > load) {
            load = load + 1;
        }
        total_nload.write(0, load);
    }

    action lookup_min_rload(node_t node) {
        load_t load;
        node_load.read(load, (bit<32>)node);
        if (load < meta.min_rload) {
            meta.dst_node = node;
            meta.min_rload = load;
        }
    }

    action extend_rep_set3() {
        meta.rkey[34:32] = 4;
        meta.rkey[50:47] = meta.min_nload_node;
        replicated_keys.write(meta.rkey_index, meta.rkey);
    }

    action extend_rep_set2() {
        meta.rkey[34:32] = 3;
        meta.rkey[46:43] = meta.min_nload_node;
        replicated_keys.write(meta.rkey_index, meta.rkey);
    }

    action extend_rep_set1() {
        meta.rkey[34:32] = 2;
        meta.rkey[42:39] = meta.min_nload_node;
        replicated_keys.write(meta.rkey_index, meta.rkey);
    }

    action init_rkey() {
        bit<64> rkey = 0;
        rkey[31:0] = hdr.pegasus.keyhash;
        rkey[34:32] = 1;
        rkey[38:35] = (bit<4>)(hdr.pegasus.keyhash & HASH_MASK);
        meta.n_replicas = 1;
        meta.dst_node = rkey[38:35];
        node_load.read(meta.min_rload, (bit<32>)meta.dst_node);
        replicated_keys.write(meta.rkey_index, rkey);
    }

    action lookup_replicated_key(bit<32> index) {
        replicated_keys.read(meta.rkey, index);
        meta.rkey_index = index;
        if (meta.rkey[31:0] != hdr.pegasus.keyhash) {
            // Replicated key has changed
            // or is uninitialized
            meta.update_rkey = 1;
        } else {
            meta.update_rkey = 0;
            meta.n_replicas = meta.rkey[34:32];
            meta.node_1 = meta.rkey[38:35];
            meta.node_2 = meta.rkey[42:39];
            meta.node_3 = meta.rkey[46:43];
            meta.node_4 = meta.rkey[50:47];
        }
    }

    table tab_replicated_keys {
        key = {
            hdr.pegasus.keyhash: exact;
        }
        actions = {
            lookup_replicated_key;
            NoAction;
        }
        size = 32;
        default_action = NoAction();
    }

    apply {
        bit<3> n_replicas;
        if (hdr.pegasus.isValid()) {
            // Read current total and min nload
            read_nload_stats();

            // Find target node based on type of operation
            if (hdr.pegasus.op == OP_REP) {
                meta.dst_node = hdr.pegasus.node;
                dec_node_load();
            } else {
                if (tab_replicated_keys.apply().hit) {
                    // If key is replicated, find the node
                    // with the minimum load to forward to
                    if (meta.update_rkey == 1) {
                        // Key is either changed or uninitialized
                        init_rkey();
                    } else {
                        meta.min_rload = MIN_LOAD_INIT_VALUE;
                        lookup_min_rload(meta.node_1);
                        n_replicas = meta.n_replicas - 1;
                        if (n_replicas > 0) {
                            lookup_min_rload(meta.node_2);
                            n_replicas = n_replicas - 1;
                            if (n_replicas > 0) {
                                lookup_min_rload(meta.node_3);
                                n_replicas = n_replicas - 1;
                                if (n_replicas > 0) {
                                    lookup_min_rload(meta.node_4);
                                }
                            }
                        }
                        // Extend the replication set if all
                        // replicas are overloaded
                        if (meta.n_replicas < MAX_REPLICAS && meta.min_rload > (meta.total_nload >> AVG_LOAD_SHIFT)) {
                            if (meta.n_replicas == 1) {
                                extend_rep_set1();
                            } else if (meta.n_replicas == 2) {
                                extend_rep_set2();
                            } else if (meta.n_replicas == 3) {
                                extend_rep_set3();
                            }
                        }
                    }
                } else {
                    // If key not replicated, forward using
                    // keyhash
                    meta.dst_node = (bit<4>)(hdr.pegasus.keyhash & HASH_MASK);
                }
                inc_node_load();
            }

            // Update min node load
            if (meta.nload < meta.min_nload) {
                update_min_nload(meta.nload, meta.dst_node);
            } else if (meta.dst_node == meta.min_nload_node) {
                // node may not be the min anymore
                meta.min_nload = meta.nload;
                check_min_nload(0);
                check_min_nload(1);
                check_min_nload(2);
                check_min_nload(3);
                update_min_nload(meta.min_nload, meta.min_nload_node);
            }

            // Set forwarding port
            if (hdr.pegasus.op == OP_REP) {
                // Reply messages use L2 forwarding
                tab_mac.apply();
            } else {
                tab_node_forward.apply();
            }
        } else if (hdr.ethernet.isValid()) {
            // All other packets use L2 forwarding
            tab_mac.apply();
        }
    }
}

/*************************************************************************
****************  E G R E S S   P R O C E S S I N G   *******************
*************************************************************************/

control MyEgress(inout headers hdr,
                 inout metadata meta,
                 inout standard_metadata_t standard_metadata) {
    apply {  }
}

/*************************************************************************
*************   C H E C K S U M    C O M P U T A T I O N   **************
*************************************************************************/

control MyComputeChecksum(inout headers hdr, inout metadata meta) {
     apply {
	update_checksum(
	    hdr.ipv4.isValid(),
            { hdr.ipv4.version,
	      hdr.ipv4.ihl,
              hdr.ipv4.diffserv,
              hdr.ipv4.totalLen,
              hdr.ipv4.identification,
              hdr.ipv4.flags,
              hdr.ipv4.fragOffset,
              hdr.ipv4.ttl,
              hdr.ipv4.protocol,
              hdr.ipv4.srcAddr,
              hdr.ipv4.dstAddr },
            hdr.ipv4.hdrChecksum,
            HashAlgorithm.csum16);
    }
}


/*************************************************************************
***********************  D E P A R S E R  *******************************
*************************************************************************/

control MyDeparser(packet_out packet, in headers hdr) {
    apply {
        packet.emit(hdr.ethernet);
        packet.emit(hdr.ipv4);
        packet.emit(hdr.udp);
        packet.emit(hdr.pegasus);
    }
}

/*************************************************************************
***********************  S W I T C H  *******************************
*************************************************************************/

V1Switch(
MyParser(),
MyVerifyChecksum(),
MyIngress(),
MyEgress(),
MyComputeChecksum(),
MyDeparser()
) main;
