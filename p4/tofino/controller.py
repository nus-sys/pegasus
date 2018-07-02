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

HASH_MASK = 0x3
RNODE_NONE = 0x7F

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
        # tab_replicated_keys, reg_rnode (1-4)
        for (keyhash, rkey_index) in tables["tab_replicated_keys"].items():
            self.client.tab_replicated_keys_table_add_with_lookup_rkey(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_replicated_keys_match_spec_t(
                    pegasus_keyhash = int(keyhash)),
                pegasus_lookup_rkey_action_spec_t(
                    action_rkey_index = rkey_index))
            self.client.register_write_reg_rnode_1(
                self.sess_hdl, self.dev_tgt, rkey_index,
                int(keyhash) & HASH_MASK)
            self.client.register_write_reg_rnode_2(
                self.sess_hdl, self.dev_tgt, rkey_index,
                RNODE_NONE)
            self.client.register_write_reg_rnode_3(
                self.sess_hdl, self.dev_tgt, rkey_index,
                RNODE_NONE)
            self.client.register_write_reg_rnode_4(
                self.sess_hdl, self.dev_tgt, rkey_index,
                RNODE_NONE)
        # reg_node_id (1-4)
        for i in range(4):
            self.client.register_write_reg_node_id_1(
                self.sess_hdl, self.dev_tgt, i,
                pegasus_reg_node_id_1_value_t(
                    f0 = i,
                    f1 = 0))
            self.client.register_write_reg_node_id_2(
                self.sess_hdl, self.dev_tgt, i,
                pegasus_reg_node_id_2_value_t(
                    f0 = i,
                    f1 = 0))
            self.client.register_write_reg_node_id_3(
                self.sess_hdl, self.dev_tgt, i,
                pegasus_reg_node_id_3_value_t(
                    f0 = i,
                    f1 = 0))
            self.client.register_write_reg_node_id_4(
                self.sess_hdl, self.dev_tgt, i,
                pegasus_reg_node_id_4_value_t(
                    f0 = i,
                    f1 = 0))

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

    controller = Controller("oyster")
    controller.install_table_entries(tables)
    controller.close()


if __name__ == "__main__":
    main()
