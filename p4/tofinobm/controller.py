import argparse
import json

from res_pd_rpc.ttypes import *
from ptf.thriftutils import *
from pegasus.p4_pd_rpc.ttypes import *
import pegasus.p4_pd_rpc.pegasus
import conn_mgr_pd_rpc.conn_mgr

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TMultiplexedProtocol

class Controller(object):
    def __init__(self, thrift_server):
        self.transport = TSocket.TSocket(thrift_server, 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        bprotocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        conn_mgr_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "conn_mgr")
        self.conn_mgr = conn_mgr_pd_rpc.conn_mgr.Client(conn_mgr_protocol)
        p4_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "pegasus")
        self.client = pegasus.p4_pd_rpc.pegasus.Client(p4_protocol)
        self.transport.open()

        self.sess_hdl = self.conn_mgr.client_init()
        self.dev = 0
        self.dev_tgt = DevTarget_t(self.dev, hex_to_i16(0xFFFF))

    def install_table_entries(self, tables):
        # tab_l2_forward
        self.client.tab_l2_forward_set_default_action__drop(
            self.sess_hdl, self.dev_tgt)
        for (mac, port) in tables["tab_l2_forward"].items():
            self.client.tab_l2_forward_table_add_with_l2_forward(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_l2_forward_match_spec_t(
                    ethernet_dstAddr = macAddr_to_string(mac)),
                pegasus_l2_forward_action_spec_t(
                    action_port = port))
        # tab_node_forward
        self.client.tab_node_forward_set_default_action__drop(
            self.sess_hdl, self.dev_tgt)
        for (node, attrs) in tables["tab_node_forward"].items():
            self.client.tab_node_forward_table_add_with_node_forward(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_node_forward_match_spec_t(
                    pegasus_node = int(node)),
                pegasus_node_forward_action_spec_t(
                    action_mac_addr = macAddr_to_string(attrs["mac"]),
                    action_ip_addr = ipv4Addr_to_i32(attrs["ip"]),
                    action_port = attrs["port"]))
        # tab_extend_rset
        self.client.tab_extend_rset_set_default_action__drop(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_extend_rset_table_add_with_extend_rset2(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_extend_rset_match_spec_t(
                meta_n_replicas = 1))
        self.client.tab_extend_rset_table_add_with_extend_rset3(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_extend_rset_match_spec_t(
                meta_n_replicas = 2))
        self.client.tab_extend_rset_table_add_with_extend_rset4(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_extend_rset_match_spec_t(
                meta_n_replicas = 3))
        # tab_update_min_rnode
        self.client.tab_update_min_rnode1_set_default_action_update_min_rnode1(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_rnode2_set_default_action_update_min_rnode2(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_rnode3_set_default_action_update_min_rnode3(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_rnode4_set_default_action_update_min_rnode4(
            self.sess_hdl, self.dev_tgt)
        # tab_init_rkey
        self.client.tab_init_rkey_set_default_action_init_rkey(
            self.sess_hdl, self.dev_tgt)
        # tab_extract_rloads
        self.client.tab_extract_rloads_set_default_action_extract_rloads(
            self.sess_hdl, self.dev_tgt)
        # tab_extract_rnodes
        self.client.tab_extract_rnodes_set_default_action_extract_rnodes(
            self.sess_hdl, self.dev_tgt)
        # tab_replicated_keys
        self.client.tab_replicated_keys_set_default_action_default_dst_node(
            self.sess_hdl, self.dev_tgt)
        for (keyhash, attrs) in tables["tab_replicated_keys"].items():
            self.client.tab_replicated_keys_table_add_with_lookup_rkey(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_replicated_keys_match_spec_t(
                    pegasus_keyhash = int(keyhash)),
                pegasus_lookup_rkey_action_spec_t(
                    action_rkey_index = attrs["rkey_index"]))
        # tab_update_min_node_load
        self.client.tab_update_min_node_load3_set_default_action_update_min_node_load3(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_node_load2_set_default_action_update_min_node_load2(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_node_load1_set_default_action_update_min_node_load1(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_node_load_from_meta_set_default_action_update_min_node_load_from_meta(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_update_min_node_load_from_hdr_set_default_action_update_min_node_load_from_hdr(
            self.sess_hdl, self.dev_tgt)
        # tab_check_min_node_load
        self.client.tab_check_min_node_load0_set_default_action_check_min_node_load0(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_check_min_node_load1_set_default_action_check_min_node_load1(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_check_min_node_load2_set_default_action_check_min_node_load2(
            self.sess_hdl, self.dev_tgt)
        self.client.tab_check_min_node_load3_set_default_action_check_min_node_load3(
            self.sess_hdl, self.dev_tgt)
        # tab_update_total_node_load
        self.client.tab_update_total_node_load_table_add_with_dec_total_node_load(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_update_total_node_load_match_spec_t(
                pegasus_op = 3))
        self.client.tab_update_total_node_load_set_default_action_inc_total_node_load(
            self.sess_hdl, self.dev_tgt)
        # tab_update_node_load
        self.client.tab_update_node_load_table_add_with_dec_node_load(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_update_node_load_match_spec_t(
                pegasus_op = 3))
        self.client.tab_update_node_load_set_default_action_inc_node_load(
            self.sess_hdl, self.dev_tgt)
        # tab_read_node_load
        self.client.tab_read_node_load_set_default_action_read_node_load(
            self.sess_hdl, self.dev_tgt)
        # tab_read_node_load_stats
        self.client.tab_read_node_load_stats_set_default_action_read_node_load_stats(
            self.sess_hdl, self.dev_tgt)
        self.conn_mgr.complete_operations(self.sess_hdl)

    def close(self):
        self.transport.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        required=True,
                        help="configuration (JSON) file")
    args = parser.parse_args()
    with open(args.config) as fp:
        tables = json.load(fp)

    controller = Controller("localhost")
    controller.install_table_entries(tables)
    controller.close()


if __name__ == "__main__":
    main()
