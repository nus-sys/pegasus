#include <tofino/constants.p4>
#include <tofino/intrinsic_metadata.p4>
#include <tofino/primitives.p4>
#include <tofino/stateful_alu_blackbox.p4>

#include "defines.p4"
#include "headers.p4"
#include "parsers.p4"
#include "tor_ser.p4"
#include "ethernet.p4"

control ingress {
    if (ig_intr_md.ingress_port == PORT_TOR_SER_TO_SERVER or ig_intr_md.ingress_port == PORT_TOR_SER_TO_SPINE) {
        apply (tor_ser_fail);
        if (tor_ser_md.fail != 1) {
            apply (tor_ser_load);
            if (dcnc.tor_load != TOR_SER_RATE_LIMIT) {
                apply (tor_ser_cache_check);
                if (tor_ser_md.exist == 1) {
                    if (dcnc.op == DCNC_READ_REQUEST) {
                        apply (tor_ser_valid_check);
                    }
                    else if (dcnc.op == DCNC_UPDATE_INVALIDATE_REQUEST) {
                        apply (tor_ser_valid_clear);
                    }
                    else if (dcnc.op == DCNC_UPDATE_VALUE_REQUEST) {
                        apply (tor_ser_valid_set);
                    }
                    if (tor_ser_md.is_valid == 1) {
                        if (dcnc.op == DCNC_READ_REQUEST) {
                            apply (tor_ser_value_read);
                        }
                        else if (dcnc.op == DCNC_UPDATE_VALUE_REQUEST) {
                            apply (tor_ser_value_update);
                        }
                    }
                }
                apply (tor_ser_route);
            }
        }
    }
}

control egress {
    apply (ethernet_set_mac);
}
