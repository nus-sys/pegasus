# Copyright 2013-present Barefoot Networks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Thrift PD interface basic tests
"""

import argparse
import json
import time
import socket
import signal
import os

from res_pd_rpc.ttypes import *
from ptf.thriftutils import *
from netcache.p4_pd_rpc.ttypes import *
import netcache.p4_pd_rpc.netcache
import conn_mgr_pd_rpc.conn_mgr

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TMultiplexedProtocol

DCNC_READ_REQUEST              = 1
DCNC_WRITE_REQUEST             = 2
DCNC_READ_REPLY                = 3
DCNC_WRITE_REPLY               = 4

def signal_handler(signum, frame):
    print "Received INT/TERM signal...Exiting"
    controller.stop()
    os._exit(1)

class Controller(object):
    def __init__(self, thrift_server):
        self.transport = TSocket.TSocket(thrift_server, 9090)
        self.transport = TTransport.TBufferedTransport(self.transport)
        bprotocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        conn_mgr_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "conn_mgr")
        self.conn_mgr = conn_mgr_pd_rpc.conn_mgr.Client(conn_mgr_protocol)
        p4_protocol = TMultiplexedProtocol.TMultiplexedProtocol(bprotocol, "netcache")
        self.client = netcache.p4_pd_rpc.netcache.Client(p4_protocol)
        self.transport.open()

        self.sess_hdl = self.conn_mgr.client_init()
        self.dev = 0
        self.dev_tgt = DevTarget_t(self.dev, hex_to_i16(0xFFFF))

    def add_key(self, key, index):
        print "adding key", key, "index", index
        self.client.tor_ser_cache_check_table_add_with_tor_ser_cache_check_act(
            self.sess_hdl, self.dev_tgt,
            netcache_tor_ser_cache_check_match_spec_t(
                dcnc_key = key),
            netcache_tor_ser_cache_check_act_action_spec_t(
                action_index = index))

    def install_table_entries(self, tables):
        # add keys
        for (key, index) in tables["cache_check"].items():
            self.add_key(key, index)
        # add l2 forwarding rules
        for (mac, port) in tables["tab_l2_forward"].items():
            self.client.tor_ser_route_l2_table_add_with_l2_forward(
                self.sess_hdl, self.dev_tgt,
                netcache_tor_ser_route_l2_match_spec_t(
                    ethernet_dstAddr = macAddr_to_string(mac)),
                netcache_l2_forward_action_spec_t(
                    action_port = port))


def main():
    global controller
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",
                        required=True,
                        help="configuration (JSON) file")
    args = parser.parse_args()
    with open(args.config) as fp:
        tables = json.load(fp)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    controller = Controller("oyster")
    controller.install_table_entries(tables)


if __name__ == "__main__":
    main()
