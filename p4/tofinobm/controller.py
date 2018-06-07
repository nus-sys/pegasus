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

    def install_table_entries(self):
        self.client.tab_l2_forward_table_add_with_l2_forward(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_l2_forward_match_spec_t(
                ethernet_dstAddr=macAddr_to_string("8a:d3:05:ce:51:67")),
            pegasus_l2_forward_action_spec_t(
                action_port=1))
        self.conn_mgr.complete_operations(self.sess_hdl)

    def close(self):
        self.transport.close()

def main():
    controller = Controller("localhost")
    controller.install_table_entries()
    controller.close()

if __name__ == "__main__":
    main()
