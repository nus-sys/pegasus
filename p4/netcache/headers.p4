#ifndef DCNC_HEADER
#define DCNC_HEADER

#include "defines.p4"

header_type ethernet_t {
    fields {
        dstAddr:    48;
        srcAddr:    48;
        etherType:  16;
    }
}
header ethernet_t ethernet;

header_type ipv4_t {
    fields {
        version:        4;
        ihl:            4;
        diffserv:       8;
        totalLen:       16;
        identification: 16;
        flags:          3;
        fragOffset:     13;
        ttl:            8;
        protocol:       8;
        hdrChecksum:    16;
        srcAddr:        32;
        dstAddr:        32;
    }
}
header ipv4_t ipv4;

header_type tcp_t {
    fields {
        srcPort:    16;
        dstPort:    16;
        seqNo:      32;
        ackNo:      32;
        dataOffset: 4;
        res:        3;
        ecn:        3;
        ctrl:       6;
        window:     16;
        checksum:   16;
        urgentPtr:  16;
    }
}
header tcp_t tcp;

header_type udp_t {
    fields {
        srcPort:    16;
        dstPort:    16;
        pkt_length: 16;
        checksum:   16;
    }
}
header udp_t udp;

header_type dcnc_t {
    fields {
        op:             8;
        key:            32;
        cache_id:       16;
        spine_padding:  7;
        spine_id:       9;
        spine_load:     32;
        tor_padding:    7;
        tor_id:         9;
        tor_load:       32;
        value:          DCNC_VALUE_WIDTH;
    }
}
header dcnc_t dcnc;

header_type dcnc_load_t {
    fields {
        load_1 : 32;
        load_2 : 32;
        load_3 : 32;
        load_4 : 32;
    }
}
header dcnc_load_t dcnc_load;


#endif
