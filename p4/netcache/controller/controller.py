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

from collections import OrderedDict

from time import sleep
import threading
import sys
import logging

import unittest

import pd_base_tests

from ptf import config
from ptf.testutils import *
from ptf.thriftutils import *

from one.p4_pd_rpc.ttypes import *
from res_pd_rpc.ttypes import *

DCNC_READ_REQUEST              = 1
DCNC_WRITE_REQUEST             = 2
DCNC_ADD_REQUEST               = 3
DCNC_DEL_REQUEST               = 4

DCNC_READ_REPLY                = 5
DCNC_READ_REPLY_NA             = 6
DCNC_WRITE_REPLY               = 7
DCNC_WRITE_REPLY_NA            = 8
DCNC_ADD_REPLY                 = 9
DCNC_DEL_REPLY                 = 10

DCNC_UPDATE_INVALIDATE_REQUEST = 11
DCNC_UPDATE_VALUE_REQUEST      = 12
DCNC_UPDATE_INVALIDATE_REPLY   = 13
DCNC_UPDATE_VALUE_REPLY        = 14

PORT_TOR_CLI_TO_CLIENT         = 188 #Physical Port 1
PORT_TOR_SER_TO_SERVER         = 184 #Physical Port 2
PORT_SPINE_TO_TOR_SER          = 284 #Physical Port 33
PORT_TOR_SER_TO_SPINE          = 280 #Physical Port 34
PORT_SPINE_TO_TOR_CLI          = 276 #Physical Port 35
PORT_TOR_CLI_TO_SPINE          = 272 #Physical Port 36

MAC_CACHE_NUM                  = 1024

dev_id = 0
dev_tgt = DevTarget_t(dev_id, hex_to_i16(0xFFFF))

OFFSET = len(str(Ether())) + len(str(IP())) + len(str(UDP()))
class DCNC(Packet):
    name = "DCNC"
    fields_desc = [
        ByteField("op", 0xff),
        IntField("key", 0),
        ShortField("cache_id", 0),
        ShortField("spine_id", 0),
        IntField("spine_load", 0),
        ShortField("tor_id", 0),
        IntField("tor_load", 0),
        IntField("value", 0),
        IntField("load_1", 0),
        IntField("load_2", 0),
        IntField("load_3", 0),
        IntField("load_4", 0),
    ]
       
class Controller(pd_base_tests.ThriftInterfaceDataPlane):
    def __init__(self):
        pd_base_tests.ThriftInterfaceDataPlane.__init__(self, ["one"])
        self.spine_count = 0
        self.tor_ser_count = 0
        self.spine_cache_hdl = {}
        for i in range(32):
            self.spine_cache_hdl[i] = []
        self.tor_ser_cache_hdl = {}
        for i in range(32):
            self.tor_ser_cache_hdl[i] = []
        self.first = 0
        self.step = 30
        
    def table_add_default(self, name):
        exec "hdl = self.client.%s_set_default_action_%s_act (self.sess_hdl, dev_tgt)" % (name, name)
    def table_add_1_1 (self, name, param_match, param_action):
        exec "match_spec = one_%s_match_spec_t ( int(param_match) )" % name
        exec "action_spec = one_%s_act_action_spec_t ( int(param_action) )" % name
        exec "hdl = self.client.%s_table_add_with_%s_act (self.sess_hdl, dev_tgt, match_spec, action_spec)" % (name, name)
        return hdl
    def table_add_2_1 (self, name, param_match_1, param_match_2, param_action):
        exec "match_spec = one_%s_match_spec_t ( int(param_match_1), int(param_match_2) )" % name
        exec "action_spec = one_%s_act_action_spec_t ( int(param_action) )" % name
        exec "hdl = self.client.%s_table_add_with_%s_act (self.sess_hdl, dev_tgt, match_spec, action_spec)" % (name, name)
        return hdl
    def register_reset (self, name):
        exec "self.client.register_reset_all_%s(self.sess_hdl, dev_tgt)" % name
        
    def initializeTorSer(self):
        #tor_ser_fail
        
        self.table_add_default("tor_ser_load")
        self.table_add_default("tor_ser_valid_check")
        self.table_add_default("tor_ser_valid_clear")
        self.table_add_default("tor_ser_valid_set")
        self.table_add_default("tor_ser_value_read")
        self.table_add_default("tor_ser_value_update")
        self.client.tor_ser_route_set_default_action_tor_ser_route_to_server(self.sess_hdl, dev_tgt)
        
        foo = open("tor_ser_keys.txt")
        foo.readline()
        self.tor_ser_count = 0
        for line in foo.readlines():
            (key, tor_ser) = line.split(' ')
            hdl = self.table_add_2_1 ('tor_ser_cache_check', tor_ser, key, self.tor_ser_count)
            #self.tor_ser_cache_hdl[tor_ser].append((hdl, self.tor_ser_count))
            self.tor_ser_count = self.tor_ser_count + 1
        foo.close()
        
        match_spec = one_tor_ser_route_match_spec_t (DCNC_READ_REPLY, 0)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_WRITE_REPLY, 0)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_UPDATE_INVALIDATE_REQUEST, 0)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_UPDATE_INVALIDATE_REQUEST, 1)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_UPDATE_VALUE_REQUEST, 0)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_UPDATE_VALUE_REQUEST, 1)
        self.client.tor_ser_route_table_add_with_tor_ser_route_to_spine (self.sess_hdl, dev_tgt, match_spec)
        match_spec = one_tor_ser_route_match_spec_t (DCNC_READ_REQUEST, 1)
        self.client.tor_ser_route_table_add_with_tor_ser_route_read_request_hit (self.sess_hdl, dev_tgt, match_spec)
    
    def initializeMAC(self):
        egress_port = PORT_TOR_CLI_TO_CLIENT #Port 1
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\x2e' #Port 1
        dmac = '\x3c\xfd\xfe\xab\xde\xd8' #netx1
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        egress_port = PORT_TOR_SER_TO_SERVER #Port 2
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\x32' #Port 2
        dmac = '\x3c\xfd\xfe\xa6\xeb\x10' #netx2
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        egress_port = PORT_SPINE_TO_TOR_SER #Port 33
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\xae' #Port 33
        dmac = '\xa8\x2b\xb5\xde\x92\xb2' #Port 34
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        egress_port = PORT_TOR_SER_TO_SPINE #Port 34
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\xb2' #Port 34
        dmac = '\xa8\x2b\xb5\xde\x92\xae' #Port 33
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        egress_port = PORT_SPINE_TO_TOR_CLI #Port 35
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\xb6' #Port 35
        dmac = '\xa8\x2b\xb5\xde\x92\xba' #Port 36
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        egress_port = PORT_TOR_CLI_TO_SPINE #Port 35
        match_spec = one_ethernet_set_mac_match_spec_t (egress_port)
        smac = '\xa8\x2b\xb5\xde\x92\xba' #Port 36
        dmac = '\xa8\x2b\xb5\xde\x92\xb6' #Port 35
        action_spec = one_ethernet_set_mac_act_action_spec_t (smac, dmac)
        self.client.ethernet_set_mac_table_add_with_ethernet_set_mac_act (self.sess_hdl, dev_tgt, match_spec, action_spec)
        
        
    def refresh(self):
        self.register_reset('tor_cli_load_tor_reg')
        self.register_reset('tor_cli_spine_load_reg')
        self.register_reset('tor_cli_least_hot_spine_reg')
        
        self.register_reset('spine_load_reg')
        
        self.register_reset('tor_ser_load_reg')
        
        #heavy_hitter
    
    def thread_refresh (self):
        while True: 
            self.refresh()
            print "Refresh Registers"
            sleep(1)
    
    def thread_hot_report (self):
        flags = one_counter_flags_t(read_hw_sync = True)
        while True:
            pkt = ' ' # Receive report from switch
            hot_read_pkt = DCNC(pkt[OFFSET:])
            min_load = min (hot_read_pkt.load_1, hot_read_pkt.load_2, hot_read_pkt.load_3, hot_read_pkt.load_4)
            
            if hot_read_pkt.tor_load > 0:
                name = 'tor_ser'
                switch_id = hot_read_pkt.tor_id
            else:
                name = 'spine'
                switch_id = hot_read_pkt.spine_id
            
            if len(self.spine_cache_hdl[switch_id]) < MAX_CACHE_NUM:
                exec "hdl = self.table_add_2_1 ('%s_cache_check', switch_id, key, self.%s_count)" % (name, name)
                exec "self.%s_cache_hdl[switch_id].append((hdl, self.%s_count))" % (name, name)
                exec "self.%s_count = self.%s_count + 1" % (name, name)
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.sendto('%d_update_hot %d' % (name, hot_read_pkt.key), ('localhost', 7878))
            else:
                exec 'cache_size = len(self.%s_cache_hdl[switch_id])' % name
                for i in range(self.first, cache_size, self.step):
                    exec 'count = self.client.counter_read_%d_cache_counter(self.sess_hdl, dev_tgt, self.%s_cache_hdl[switch_id][i][0], flags)' % (name, name)
                    if count < min_load:
                        break
                    
                if i < cache_size:
                    exec '(hdl, index) = self.%s_cache_hdl[switch_id][i]' % name
                    exec 'self.client.register_write_%s_valid_reg(self.sess_hdl, dev_tgt, index, 0)' % name
                    exec 'self.client.%s_cache_check_table_delete(self.sess_hdl, dev_id, hdl)' % name
                    exec "hdl = self.table_add_2_1 ('%s_cache_check', switch_id, key, self.%s_count)" % (name, name)
                    exec "self.spine_cache_hdl[switch_id][i][0] = hdl" % name
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.sendto('%d_update_hot %d' % (name, hot_read_pkt.key), ('localhost', 7878))
                
            self.first = (self.first + 1) % self.step
    
    def control_spine_fail (self, spine):
        match_spec = one_spine_fail_match_spec(spine)
        self.client.spine_fail_table_add_with_spine_fail_act(self.sess_hdl, dev_tgt, match_spec)
    
    def control_add_table_entry_1_1 (self, name, param_match, param_action):
        self.table_add_1_1 (name, param_match, param_action)
    
            
    def thread_control (self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('localhost', 7878))
        while True:
            order = s.recv(2048)
            print order
            
            args = order.split(' ')
            if args[0] == 'spine_fail':
                match_spec = one_spine_fail_match_spec( int(args[1]) )
                self.client.spine_fail_table_add_with_spine_fail_act(self.sess_hdl, dev_tgt, match_spec)
            elif args[0] == 'tor_ser_fail':
                match_spec = one_tor_ser_fail_match_spec( int(args[1]) )
                self.client.tor_ser_fail_table_add_with_tor_ser_fail_act(self.sess_hdl, dev_tgt, match_spec)
            elif args[0] == 'spine_insert_cache':
                self.table_add_2_1 ('spine_cache_check', args[1], args[2], self.spine_count)
                self.spine_count = self.spine_count + 1
            elif args[0] == 'tor_ser_insert_cache':
                self.table_add_2_1 ('tor_ser_cache_check', args[1], args[2], self.tor_ser_count)
                self.tor_ser_count = self.tor_ser_count + 1
                
        
    """ Basic test """
    def runTest(self):
        self.sess_hdl = self.conn_mgr.client_init()
        self.initializeTorSer()
        self.initializeMAC()
        
        self.threads = []
        t1 = threading.Thread(target = self.thread_refresh, args = ())
        self.threads.append(t1)
        t2 = threading.Thread(target = self.thread_control, args = ())
        self.threads.append(t2)
        #t3 = threading.Thread(target = self.thread_hot_read, args = ())
        #self.threads.append(t3)
        
        print "Initialized"
        
        try:
            self.conn_mgr.complete_operations(self.sess_hdl)
            
            for t in self.threads:
                t.setDaemon(True)
                t.start()
                
            while True:
                sleep(1)
            
            print "Tested"

        finally:
            print "Complete"
