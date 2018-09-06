action ethernet_set_mac_act (smac, dmac) {
    modify_field (ethernet.srcAddr, smac);
    modify_field (ethernet.dstAddr, dmac);
}
table ethernet_set_mac {
    reads {
        ig_intr_md_for_tm.ucast_egress_port: exact;
    }
    actions {
        ethernet_set_mac_act;
    }
}
