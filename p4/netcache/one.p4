#include <tofino/constants.p4>
#include <tofino/intrinsic_metadata.p4>
#include <tofino/primitives.p4>
#include <tofino/stateful_alu_blackbox.p4>

#include "defines.p4"
#include "headers.p4"
#include "parsers.p4"
#include "tor_ser.p4"

control ingress {
    if (valid(dcnc)) {
        apply (tor_ser_cache_check);
        apply (tor_ser_copy_header);
        if (tor_ser_md.exist == 1) {
            if (dcnc.op == DCNC_READ_REQUEST) {
                apply (tor_ser_valid_check);
            }
            else if (dcnc.op == DCNC_WRITE_REQUEST) {
                apply (tor_ser_valid_clear);
            }
            else if (dcnc.op == DCNC_WRITE_REPLY or dcnc.op == DCNC_READ_REPLY) {
                apply (tor_ser_valid_set);
            }
            if (tor_ser_md.is_valid == 1) {
                if (dcnc.op == DCNC_READ_REQUEST) {
                    apply (tor_ser_value_read);
                }
                else if (dcnc.op == DCNC_WRITE_REPLY or dcnc.op == DCNC_READ_REPLY) {
                    apply (tor_ser_value_update);
                }
            }
        }
    }
    apply (tor_ser_route_l2);
}

control egress {
}
