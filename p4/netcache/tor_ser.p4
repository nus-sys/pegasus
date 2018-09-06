header_type tor_ser_md_t {
    fields {
        index:      17;
        exist:      1;
        is_valid:   1;
        ethernet_addr: 48;
        ipv4_addr: 32;
        udp_port: 16;
    }
}
metadata tor_ser_md_t tor_ser_md;

action tor_ser_cache_check_act(index) {
    modify_field(tor_ser_md.index, index);
    modify_field(tor_ser_md.exist, 1);
}
@pragma stage 1
table tor_ser_cache_check {
    reads {
        dcnc.key: exact;
    }
    actions {
        tor_ser_cache_check_act;
    }
    size : 131072;
}

action tor_ser_copy_header_act() {
    modify_field(tor_ser_md.ethernet_addr, ethernet.srcAddr);
    modify_field(tor_ser_md.ipv4_addr, ipv4.srcAddr);
    modify_field(tor_ser_md.udp_port, udp.srcPort);
}
@pragma stage 1
table tor_ser_copy_header {
    actions {
        tor_ser_copy_header_act;
    }
    default_action: tor_ser_copy_header_act;
    size : 1;
}

@pragma stage 2
register tor_ser_valid_reg {
    width: 1;
    instance_count: 131072;
}
blackbox stateful_alu tor_ser_valid_check_alu {
    reg: tor_ser_valid_reg;
    output_value: register_lo;
    output_dst: tor_ser_md.is_valid;
}
action tor_ser_valid_check_act () {
    tor_ser_valid_check_alu.execute_stateful_alu (tor_ser_md.index);
}
@pragma stage 2
table tor_ser_valid_check {
    actions {
        tor_ser_valid_check_act;
    }
    default_action: tor_ser_valid_check_act;
}
blackbox stateful_alu tor_ser_valid_clear_alu {
    reg: tor_ser_valid_reg;
    update_lo_1_value: clr_bit;
}
action tor_ser_valid_clear_act () {
    tor_ser_valid_clear_alu.execute_stateful_alu (tor_ser_md.index);
}
@pragma stage 2
table tor_ser_valid_clear {
    actions {
        tor_ser_valid_clear_act;
    }
    default_action: tor_ser_valid_clear_act;
}
blackbox stateful_alu tor_ser_valid_set_alu {
    reg: tor_ser_valid_reg;
    update_lo_1_value: set_bitc;
    output_value: alu_lo;
    output_dst: tor_ser_md.is_valid;
}
action tor_ser_valid_set_act () {
    tor_ser_valid_set_alu.execute_stateful_alu (tor_ser_md.index);
}
@pragma stage 2
table tor_ser_valid_set {
    actions {
        tor_ser_valid_set_act;
    }
    default_action: tor_ser_valid_set_act;
}

@pragma stage 3
register tor_ser_value_reg {
    width: 32;
    instance_count: 131072;
}
blackbox stateful_alu tor_ser_value_read_alu {
    reg: tor_ser_value_reg;
    output_value: register_lo;
    output_dst: dcnc.value;
}
action tor_ser_value_read_act () {
    tor_ser_value_read_alu.execute_stateful_alu (tor_ser_md.index);
    // Exchange src addresses with dst addresses
    modify_field(ethernet.srcAddr, ethernet.dstAddr);
    modify_field(ethernet.dstAddr, tor_ser_md.ethernet_addr);
    modify_field(ipv4.srcAddr, ipv4.dstAddr);
    modify_field(ipv4.dstAddr, tor_ser_md.ipv4_addr);
    modify_field(udp.srcPort, udp.dstPort);
    modify_field(udp.dstPort, tor_ser_md.udp_port);
    // Set op
    modify_field (dcnc.op, DCNC_READ_REPLY);
}
@pragma stage 3
table tor_ser_value_read {
    actions {
        tor_ser_value_read_act;
    }
    default_action: tor_ser_value_read_act;
}
blackbox stateful_alu tor_ser_value_update_alu {
    reg: tor_ser_value_reg;
    update_lo_1_value: dcnc.value;
}
action tor_ser_value_update_act () {
    tor_ser_value_update_alu.execute_stateful_alu (tor_ser_md.index);
}
@pragma stage 3
table tor_ser_value_update {
    actions {
        tor_ser_value_update_act;
    }
    default_action: tor_ser_value_update_act;
}

action _drop() {
    drop();
}
action l2_forward(port) {
    modify_field(ig_intr_md_for_tm.ucast_egress_port, port);
}

table tor_ser_route_l2 {
    reads {
        ethernet.dstAddr: exact;
    }
    actions {
        l2_forward;
        _drop;
    }
    default_action: _drop;
    size: 1024;
}
