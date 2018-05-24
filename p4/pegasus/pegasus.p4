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

const bit<16> TYPE_IPV4 = 0x800;
const bit<8> PROTO_UDP = 0x11;
const op_t OP_GET = 0x0;
const op_t OP_PUT = 0x1;
const op_t OP_DEL = 0x2;
const udpPort_t PEGASUS_PORT = 0xBEDA;

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
    op_t        op;
    keyhash_t   keyhash;
}

struct metadata {
    /* empty */
}

struct headers {
    ethernet_t   ethernet;
    ipv4_t       ipv4;
    udp_t        udp;
    pegasus_t    pegasus;
}

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
        transition select (hdr.udp.dstPort) {
            PEGASUS_PORT: parse_pegasus;
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

    table mac {
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

    action rkey_forward(egressSpec_t port) {
        standard_metadata.egress_spec = port;
    }

    table replicated_keys {
        key = {
            hdr.pegasus.op: exact;
        }
        actions = {
            rkey_forward;
            NoAction;
        }
        size = 8;
        default_action = NoAction();
    }

    apply {
        if (hdr.pegasus.isValid()) {
            if (!replicated_keys.apply().hit) {
                // If key not replicated, forward using L2
                mac.apply();
            }
        } else if (hdr.ethernet.isValid()) {
            // All other packets use L2 forwarding
            mac.apply();
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
         /* empty */
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
