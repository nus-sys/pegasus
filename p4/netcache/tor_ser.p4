#define TOR_SER_RATE_LIMIT 35000

header_type tor_ser_md_t {
    fields {
        fail:       1;
        index:      17;
        exist:      1;
        is_valid:   1;
    }
}
metadata tor_ser_md_t tor_ser_md;

action tor_ser_fail_act() {
    modify_field (tor_ser_md.fail, 1);
    drop();
}
@pragma stage 1
table tor_ser_fail {
    reads {
        dcnc.tor_id: exact;
    }
    actions {
        tor_ser_fail_act;
    }
}

@pragma stage 2
register tor_ser_load_reg {
    width: 32;
    instance_count: 128;
}
blackbox stateful_alu tor_ser_load_alu {
    reg: tor_ser_load_reg;
    condition_lo: register_lo < TOR_SER_RATE_LIMIT;
    
    update_lo_1_predicate: condition_lo;
    update_lo_1_value: register_lo + 1;
    
    output_value: register_lo;
    output_dst: dcnc.tor_load;
}
action tor_ser_load_act () {
    tor_ser_load_alu.execute_stateful_alu (dcnc.tor_id);
}
@pragma stage 2
table tor_ser_load {
    actions {
        tor_ser_load_act;
    }
    default_action: tor_ser_load_act;
}

action tor_ser_cache_check_act(index) {
    modify_field(tor_ser_md.index, index);
    modify_field(tor_ser_md.exist, 1);
}
@pragma stage 3
table tor_ser_cache_check {
    reads {
        dcnc.tor_id: exact;
        dcnc.key: exact;
    }
    actions {
        tor_ser_cache_check_act;
    }
    size : 131072;
}

@pragma stage 4
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
@pragma stage 4
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
@pragma stage 4
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
@pragma stage 4
table tor_ser_valid_set {
    actions {
        tor_ser_valid_set_act;
    }
    default_action: tor_ser_valid_set_act;
}

@pragma stage 5
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
}
@pragma stage 5
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
@pragma stage 5
table tor_ser_value_update {
    actions {
        tor_ser_value_update_act;
    }
    default_action: tor_ser_value_update_act;
}

action tor_ser_route_to_spine() {
    modify_field(ig_intr_md_for_tm.ucast_egress_port, PORT_TOR_SER_TO_SPINE);
}
action tor_ser_route_to_server() {
    modify_field(ig_intr_md_for_tm.ucast_egress_port, PORT_TOR_SER_TO_SERVER);
}
action tor_ser_route_read_request_hit() {
    modify_field (dcnc.op, DCNC_READ_REPLY);
    modify_field (ipv4.dstAddr, ipv4.srcAddr);
    modify_field(ig_intr_md_for_tm.ucast_egress_port, PORT_TOR_SER_TO_SPINE);
}

@pragma stage 5
table tor_ser_route {
    reads {
        dcnc.op: exact;
        tor_ser_md.is_valid: exact;
    }
    actions {
        tor_ser_route_to_spine;
        tor_ser_route_to_server;
        tor_ser_route_read_request_hit;
    }
    default_action: tor_ser_route_to_server;
}
