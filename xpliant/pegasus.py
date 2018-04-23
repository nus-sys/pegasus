#!/usr/bin/env python
import threading
import socket
import struct
import math
from buildTarget import *

ADDR = ('10.1.1.201', 6780)
BUFSIZE = 1024

# Packet header format
IDENTIFIER_SIZE = 4
TYPE_SIZE = 1
NUM_NODES_SIZE = 4
ACK_SIZE = 1

PACKET_BASE_SIZE = IDENTIFIER_SIZE + TYPE_SIZE
RESET_SIZE = PACKET_BASE_SIZE + NUM_NODES_SIZE
REPLY_SIZE = PACKET_BASE_SIZE + ACK_SIZE

IDENTIFIER = 0xDEADDEAD
TYPE_RESET = 0x0
TYPE_REPLY = 0x1
ACK_OK = 0x0
ACK_FAILED = 0x1

class ControllerMessage(object):
  def __init__(self, num_nodes=0):
    self.num_nodes = num_nodes


class PegasusController(threading.Thread):
  class Node(object):
    def __init__(self, prefix=0, mask=0, prefix_idx=0):
      self.prefix = prefix
      self.mask = mask
      self.prefix_idx = prefix_idx

  def __init__(self):
    threading.Thread.__init__(self)
    self.sk = socket.socket(type=socket.SOCK_DGRAM)
    self.sk.bind(ADDR)
    self.nodes = []

  def run(self):
    while True:
      (msg, addr) = self.sk.recvfrom(BUFSIZE)
      ctrl_msg = ControllerMessage()
      if self.parse_msg(msg, ctrl_msg):
        self.process_ctrl_msg(ctrl_msg)
        self.reply_ok(addr)

  def process_ctrl_msg(self, msg):
    # Remove existing routes
    for node in self.nodes:
      self.remove_route_lpm(node.prefix, node.mask, node.prefix_idx)
    self.nodes = []

    # Add new routes
    nbits = int(math.log(msg.num_nodes, 2))
    mask = 8 * 12 + nbits
    for i in range(msg.num_nodes):
      prefix = [0] * 12
      prefix.append(i << (8 - nbits))
      prefix.extend([0] * 3)
      node = self.Node(prefix=prefix, mask=mask)
      node.prefix_idx = self.add_route_lpm(prefix, mask, i)
      self.nodes.append(node)

  def parse_msg(self, msg, ctrl_msg):
    if len(msg) < PACKET_BASE_SIZE:
      return False
    index = 0
    identifier = struct.unpack('<I', msg[index:index+IDENTIFIER_SIZE])[0]
    if identifier != IDENTIFIER:
      return False
    index += IDENTIFIER_SIZE
    msg_type = struct.unpack('<B', msg[index:index+TYPE_SIZE])[0]
    if msg_type != TYPE_RESET:
      return False
    index += TYPE_SIZE
    if len(msg) < RESET_SIZE:
      return False
    ctrl_msg.num_nodes = struct.unpack('<I', msg[index:index+NUM_NODES_SIZE])[0]
    return True

  def reply_ok(self, addr):
    msg = ''
    msg += struct.pack('<I', IDENTIFIER)
    msg += struct.pack('<B', TYPE_REPLY)
    msg += struct.pack('<B', ACK_OK)
    self.sk.sendto(msg, addr)

  def add_route_lpm(self, prefix, mask, nhid):
    entry = new_xpLpmEntryp()
    entry.prfxType = 0
    entry.vrfId = 0
    ip_addr = new_arr4_tp(16)
    list_len = len(prefix)
    for ix in range(list_len-1, -1, -1):
      arr4_tp_setitem(ip_addr, (list_len-ix-1), prefix[ix])
    entry.addr = ip_addr
    prefix_idx = new_uint32_tp()
    is_new_subtrie = new_uint8_tp()
    ret = xpIpv6RouteLpmAddEntry(0, entry, mask, 1, nhid, prefix_idx,
        is_new_subtrie)
    if ret != 0:
      print("Failed to add route entry")
    index = uint32_tp_value(prefix_idx)
    delete_uint8_tp(is_new_subtrie)
    delete_uint32_tp(prefix_idx)
    delete_xpLpmEntryp(entry)
    return index

  def remove_route_lpm(self, prefix, mask, prefix_index):
    entry = new_xpLpmEntryp()
    entry.prfxType = 0
    entry.vrfId = 0
    ip_addr = new_arr4_tp(16)
    list_len = len(prefix)
    for ix in range(list_len-1, -1, -1):
      arr4_tp_setitem(ip_addr, (list_len-ix-1), prefix[ix])
    entry.addr = ip_addr
    ret = xpIpv6RouteLpmRemoveEntry(0, entry, mask, prefix_index)
    if ret != 0:
      print("Failed to remove route entry")
    delete_xpLpmEntryp(entry)
