import argparse
import json
import time
import socket
import threading
import struct
import signal
import os

from sortedcontainers import SortedDict
from sortedcontainers import SortedList

from res_pd_rpc.ttypes import *
from ptf.thriftutils import *
from pegasus.p4_pd_rpc.ttypes import *
from devport_mgr_pd_rpc.ttypes import *
import pegasus.p4_pd_rpc.pegasus
import conn_mgr_pd_rpc.conn_mgr
import devport_mgr_pd_rpc.devport_mgr

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TMultiplexedProtocol

CONTROLLER_ADDR = ("198.19.3.16", 12345)
THRIFT_SERVER = "localhost"
THRIFT_PORT = 9090

IDENTIFIER = 0xDEAC
IDENTIFIER_SIZE = 2
TYPE_ERROR = -1
TYPE_RESET_REQ = 0x0
TYPE_REPLICATION = 0x3
TYPE_STATS = 0x4
TYPE_SIZE = 1
NNODES_SIZE = 2
NKEYS_SIZE = 2
KEYHASH_SIZE = 4
LOAD_SIZE = 2
KEY_LEN_SIZE = 2

BUF_SIZE = 4096
MAX_NRKEYS = 64
DEFAULT_NUM_NODES = 32
MAX_RSET_SIZE = 32
RSET_INDEX_SHIFT = 5

PRINT_DEBUG = True
RSET_ALL = True

g_controller = None

def signal_handler(signum, frame):
    print "Received INT/TERM signal...Exiting"
    g_controller.stop()
    os._exit(1)

# Messages
class ResetRequest(object):
    def __init__(self, num_nodes, num_rkeys):
        self.num_nodes = num_nodes
        self.num_rkeys = num_rkeys

class ControllerReplication(object):
    def __init__(self, keyhash, key):
        self.keyhash = keyhash
        self.key = key

# Data structure for replicated keys
class ReplicatedKey(object):
    def __init__(self, index, load):
        self.index = index
        self.load = load
        self.nodes = set()

# Message Codec
def decode_msg(buf):
    index = 0
    identifier = struct.unpack('<H', buf[index:index+IDENTIFIER_SIZE])[0]
    if identifier != IDENTIFIER:
        return (TYPE_ERROR, None)
    index += IDENTIFIER_SIZE
    msg_type = struct.unpack('<B', buf[index:index+TYPE_SIZE])[0]
    index += TYPE_SIZE
    if msg_type == TYPE_RESET_REQ:
        num_nodes = struct.unpack('<H', buf[index:index+NNODES_SIZE])[0]
        index += NNODES_SIZE
        num_rkeys = struct.unpack('<H', buf[index:index+NKEYS_SIZE])[0]
        msg = ResetRequest(num_nodes, num_rkeys)
    elif msg_type == TYPE_STATS:
        msg = None
    else:
        msg_type = TYPE_ERROR
        msg = None
    return (msg_type, msg)

def encode_msg(msg_type, msg):
    buf = ""
    buf += struct.pack('<H', IDENTIFIER)
    buf += struct.pack('<B', msg_type)
    if msg_type == TYPE_REPLICATION:
        buf += struct.pack('<I', msg.keyhash)
        buf += struct.pack('<H', len(msg.key))
        buf += msg.key
    else:
        buf = ""
    return buf

# Message handler
class MessageHandler(threading.Thread):
    def __init__(self, address, controller):
        threading.Thread.__init__(self)
        self.controller = controller
        self.ctrl_sk = socket.socket(type=socket.SOCK_DGRAM)
        self.ctrl_sk.bind(address)

    def handle_reset_request(self, addr, msg):
        print "Reset request with", msg.num_nodes, "nodes", msg.num_rkeys, "rkeys"
        self.controller.num_nodes = msg.num_nodes
        self.controller.num_rkeys = msg.num_rkeys
        self.controller.reset()

    def run(self):
        while True:
            (buf, addr) = self.ctrl_sk.recvfrom(BUF_SIZE)
            (msg_type, msg) = decode_msg(buf)
            if msg_type == TYPE_RESET_REQ:
                self.handle_reset_request(addr, msg)
            elif msg_type == TYPE_STATS:
                self.controller.print_stats()

# Main controller
class Controller(object):
    def __init__(self, thrift_server, thrift_port):
        self.transport = TSocket.TSocket(thrift_server, thrift_port)
        self.transport = TTransport.TBufferedTransport(self.transport)
        bprotocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        conn_mgr_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "conn_mgr")
        self.conn_mgr = conn_mgr_pd_rpc.conn_mgr.Client(conn_mgr_protocol)
        p4_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "pegasus")
        self.pegasus = pegasus.p4_pd_rpc.pegasus.Client(p4_protocol)
        self.devport = devport_mgr_pd_rpc.devport_mgr.Client(conn_mgr_protocol)
        self.transport.open()

        self.sess_hdl = self.conn_mgr.client_init()
        self.dev = 0
        self.dev_tgt = DevTarget_t(self.dev, hex_to_i16(0xFFFF))
        self.flags = pegasus_register_flags_t(read_hw_sync=True)

        # keyhash -> ReplicatedKey (sorted in ascending load)
        self.replicated_keys = SortedDict(lambda x : self.replicated_keys[x].load)
        self.num_nodes = DEFAULT_NUM_NODES
        self.num_rkeys = MAX_NRKEYS
        self.switch_lock = threading.Lock()

    def install_table_entries(self, tables):
        # tab_l2_forward
        self.pegasus.tab_l2_forward_set_default_action__drop(
            self.sess_hdl, self.dev_tgt)
        for (mac, port) in tables["tab_l2_forward"].items():
            self.pegasus.tab_l2_forward_table_add_with_l2_forward(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_l2_forward_match_spec_t(
                    ethernet_dstAddr = macAddr_to_string(mac)),
                pegasus_l2_forward_action_spec_t(
                    action_port = port))
        # tab_node_forward
        self.pegasus.tab_node_forward_set_default_action__drop(
            self.sess_hdl, self.dev_tgt)
        self.num_nodes = len(tables["tab_node_forward"])
        for (node, attrs) in tables["tab_node_forward"].items():
            self.pegasus.tab_node_forward_table_add_with_node_forward(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_node_forward_match_spec_t(
                    meta_node = int(node)),
                pegasus_node_forward_action_spec_t(
                    action_mac_addr = macAddr_to_string(attrs["mac"]),
                    action_ip_addr = ipv4Addr_to_i32(attrs["ip"]),
                    action_udp_addr = attrs["udp"],
                    action_port = attrs["port"]))
        # reg_n_servers
        self.pegasus.register_write_reg_n_servers(
            self.sess_hdl, self.dev_tgt, 0, self.num_nodes)
        # tab_calc_rset_index
        for i in range(self.num_rkeys):
            self.pegasus.tab_calc_rset_index_table_add_with_calc_rset_index(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_calc_rset_index_match_spec_t(
                    meta_rkey_index = i),
                pegasus_calc_rset_index_action_spec_t(
                    action_base = i * MAX_RSET_SIZE))
        # tab_replicated_keys
        self.all_rkeys = tables["tab_replicated_keys"]
        for i in range(self.num_rkeys):
            self.add_rkey(self.all_rkeys[i], i, 0)
        self.conn_mgr.complete_operations(self.sess_hdl)

    def reset(self):
        self.switch_lock.acquire()
        for keyhash in self.replicated_keys.keys():
            self.pegasus.tab_replicated_keys_table_delete_by_match_spec(
                self.sess_hdl, self.dev_tgt,
                pegasus_tab_replicated_keys_match_spec_t(
                    pegasus_keyhash = int(keyhash)))
        self.replicated_keys.clear()
        for i in range(self.num_rkeys):
            self.add_rkey(self.all_rkeys[i], i, 0)
        self.pegasus.register_write_reg_ver_next(
            self.sess_hdl, self.dev_tgt, 0, 1)
        self.pegasus.register_write_reg_n_servers(
            self.sess_hdl, self.dev_tgt, 0, self.num_nodes)
        self.pegasus.register_write_reg_rr_all_servers(
            self.sess_hdl, self.dev_tgt, 0, 0)
        self.conn_mgr.complete_operations(self.sess_hdl)
        self.switch_lock.release()

    def add_rkey(self, keyhash, rkey_index, load):
        if RSET_ALL:
            self.pegasus.register_write_reg_rset_size(
                self.sess_hdl, self.dev_tgt, rkey_index, self.num_nodes)
            bitmap = (2**self.num_nodes) - 1
            self.pegasus.register_write_reg_rset_bitmap(
                self.sess_hdl, self.dev_tgt, rkey_index, bitmap)
            rset_index = rkey_index << RSET_INDEX_SHIFT
            for i in range(self.num_nodes):
                self.pegasus.register_write_reg_rset(
                    self.sess_hdl, self.dev_tgt, rset_index + i, i)
        else:
            node = int(keyhash) % self.num_nodes
            bitmap = 1 << node
            rset_index = rkey_index << RSET_INDEX_SHIFT
            self.pegasus.register_write_reg_rset_size(
                self.sess_hdl, self.dev_tgt, rkey_index, 1)
            self.pegasus.register_write_reg_rset_bitmap(
                self.sess_hdl, self.dev_tgt, rkey_index, bitmap)
            self.pegasus.register_write_reg_rset(
                self.sess_hdl, self.dev_tgt, rset_index, node)
        self.pegasus.register_write_reg_rkey_ver_completed(
            self.sess_hdl, self.dev_tgt, rkey_index, 1)
        self.pegasus.register_write_reg_rkey_read_counter(
            self.sess_hdl, self.dev_tgt, rkey_index, 0)
        self.pegasus.register_write_reg_rkey_write_counter(
            self.sess_hdl, self.dev_tgt, rkey_index, 0)
        self.pegasus.register_write_reg_rkey_rate_counter(
            self.sess_hdl, self.dev_tgt, rkey_index, 0)
        self.pegasus.register_write_reg_rr_rkey(
            self.sess_hdl, self.dev_tgt, rkey_index, 0)
        self.pegasus.tab_replicated_keys_table_add_with_is_rkey(
            self.sess_hdl, self.dev_tgt,
            pegasus_tab_replicated_keys_match_spec_t(
                pegasus_keyhash = int(keyhash)),
            pegasus_is_rkey_action_spec_t(
                action_rkey_index = rkey_index))
        self.replicated_keys.setdefault(keyhash, ReplicatedKey(index=int(rkey_index),
                                                               load=load))

    def periodic_update(self):
        self.switch_lock.acquire()
        # Reset read and write counters
        self.pegasus.register_reset_all_reg_rkey_read_counter(
            self.sess_hdl, self.dev_tgt)
        self.pegasus.register_reset_all_reg_rkey_write_counter(
            self.sess_hdl, self.dev_tgt)
        # Read rkey load
        for rkey in self.replicated_keys.values():
            read_value = self.pegasus.register_read_reg_rkey_rate_counter(
                self.sess_hdl, self.dev_tgt, rkey.index, self.flags)
            rkey.load = int(read_value[1])
        # Reset rkey load
        self.pegasus.register_reset_all_reg_rkey_rate_counter(
            self.sess_hdl, self.dev_tgt)
        self.conn_mgr.complete_operations(self.sess_hdl)
        self.switch_lock.release()

    def print_stats(self):
        self.switch_lock.acquire()
        # read replicated keys info
        for (keyhash, rkey) in self.replicated_keys.items():
            read_value = self.pegasus.register_read_reg_rkey_rate_counter(
                self.sess_hdl, self.dev_tgt, rkey.index, self.flags)
            rkey.load = read_value[0]
            print "rkey hash", keyhash
            print "rkey load", rkey.load
            read_value = self.pegasus.register_read_reg_rkey_ver_completed(
                self.sess_hdl, self.dev_tgt, rkey.index, self.flags)
            print "ver completed", read_value[0]
            read_value = self.pegasus.register_read_reg_rset_size(
                self.sess_hdl, self.dev_tgt, rkey.index, self.flags)
            rset_size = int(read_value[0])
            print "rset size", rset_size
            read_value = self.pegasus.register_read_reg_rset_bitmap(
                self.sess_hdl, self.dev_tgt, rkey.index, self.flags)
            print "rset bitmap", read_value[0]
            base = rkey.index * MAX_RSET_SIZE
            for i in range(rset_size):
                read_value = self.pegasus.register_read_reg_rset(
                    self.sess_hdl, self.dev_tgt, base+i, self.flags)
                print "replica", read_value[0]
        self.conn_mgr.complete_operations(self.sess_hdl)
        self.switch_lock.release()

    def run(self):
        while True:
            time.sleep(0.1)
            self.periodic_update()

    def stop(self):
        self.transport.close()

def main():
    global g_controller
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        required=True,
                        help="configuration (JSON) file")

    args = parser.parse_args()
    with open(args.config) as fp:
        tables = json.load(fp)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    controller = Controller(THRIFT_SERVER, THRIFT_PORT)
    g_controller = controller
    handler = MessageHandler(CONTROLLER_ADDR, controller)

    controller.install_table_entries(tables)
    handler.start()
    controller.run()
    handler.join()
    controller.stop()

if __name__ == "__main__":
    main()
