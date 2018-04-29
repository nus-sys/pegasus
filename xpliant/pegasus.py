#!/usr/bin/env python
import threading
import socket
import struct
import math
from buildTarget import *

ADDR = ('10.1.1.201', 6780)
BUFSIZE = 1024
HASH_SIZE = 32

# Packet header format
IDENTIFIER_SIZE = 4
TYPE_SIZE = 1
NUM_NODES_SIZE = 4
ACK_SIZE = 1
KEYHASH_SIZE = 4
NODE_ID_SIZE = 4

PACKET_BASE_SIZE = IDENTIFIER_SIZE + TYPE_SIZE
RESET_REQ_SIZE = PACKET_BASE_SIZE + NUM_NODES_SIZE
RESET_REPLY_SIZE = PACKET_BASE_SIZE + ACK_SIZE
MIGRATION_REQ_SIZE = PACKET_BASE_SIZE + 2 * KEYHASH_SIZE + NODE_ID_SIZE
MIGRATION_REPLY_SIZE = PACKET_BASE_SIZE + ACK_SIZE
REGISTER_REQ_SIZE = PACKET_BASE_SIZE + NODE_ID_SIZE
REGISTER_REPLY_SIZE = PACKET_BASE_SIZE + 2 * KEYHASH_SIZE

IDENTIFIER = 0xDEADDEAD
TYPE_RESET_REQ        = 0x0
TYPE_RESET_REPLY      = 0x1
TYPE_MIGRATION_REQ    = 0x2
TYPE_MIGRATION_REPLY  = 0x3
TYPE_REGISTER_REQ     = 0x4
TYPE_REGISTER_REPLY   = 0x5
ACK_OK = 0x0
ACK_FAILED = 0x1


def encode_hash_range(start, end):
  entries = []
  marker = 1 << (HASH_SIZE - 1)
  s_marker = 0
  while marker > 0:
    a = start & marker
    b = end & marker
    if a != b:
      s_maker = marker
      break
    marker = marker >> 1
  if s_marker == 0:
    return [(end, HASH_SIZE)]

  curr_end = start
  bit = 0
  marker = 1 << bit
  while marker < s_marker:
    if marker & curr_end != 0:
      prefixs.append((curr_end, HASH_SIZE-bit))
      curr_end = curr_end + marker
    bit += 1
    marker = 1 << bit
  if curr_end & marker == 0:
    if end & ((1 << (bit+1)) - 1) == (1 << (bit+1)) - 1:
      return [(curr_end, HASH_SIZE-bit-1)]
    prefixs.append((curr_end, HASH_SIZE-bit))

  target = (end >> bit) << bit
  curr_end = end
  bit = 0
  marker = 1 << bit
  while marker < s_marker:
    if curr_end == target:
      prefixs.append((curr_end, HASH_SIZE-bit))
      return prefixs
    if marker & curr_end == 0:
      prefixs.append((curr_end, HASH_SIZE-bit))
      curr_end = curr_end - marker
    bit += 1
    marker = 1 << bit
    curr_end = (curr_end >> bit) << bit

  prefixs.append((curr_end, HASH_SIZE-bit))
  return prefixs


class ControllerMessage(object):
  class ResetRequest(object):
    def __init__(self, num_nodes=0):
      self.num_nodes = num_nodes

  class ResetReply(object):
    def __init__(self, ack=ACK_OK):
      self.ack = ack

  class MigrationRequest(object):
    def __init__(self, start=0, end=0, dst_node_id=0):
      self.start = start
      self.end = end
      self.dst_node_id = dst_node_id

  class MigrationReply(object):
    def __init__(self, ack=ACK_OK):
      self.ack = ack

  class RegisterRequest(object):
    def __init__(self, node_id=0):
      self.node_id = node_id

  class RegisterReply(object):
    def __init__(self, start=0, end=0):
      self.start = start
      self.end = end

  def __init__(self, msg_type=None, msg=None):
    self.msg_type = msg_type
    self.msg = msg


class PegasusController(threading.Thread):
  class NodeHash(object):
    def __init__(self, start=None, end=None, prefix=None, mask=None, prefix_idx=None):
      self.start = start
      self.end = end
      self.prefix = prefix
      self.mask = mask
      self.prefix_idx = prefix_idx

  class Node(object):
    def __init__(self, addr=None):
      self.addr = addr
      self.node_hashes = []

  def __init__(self):
    threading.Thread.__init__(self)
    self.sk = socket.socket(type=socket.SOCK_DGRAM)
    self.sk.bind(ADDR)
    self.nodes = {} # node_id -> Node

  def run(self):
    while True:
      (msg, addr) = self.sk.recvfrom(BUFSIZE)
      ctrl_msg = ControllerMessage()
      if self.parse_msg(msg, ctrl_msg):
        self.process_ctrl_msg(ctrl_msg, addr)

  def process_ctrl_msg(self, ctrl_msg, addr):
    if ctrl_msg.msg_type == TYPE_RESET_REQ:
      self.process_reset_req(ctrl_msg.msg, addr)
    elif ctrl_msg.msg_type == TYPE_MIGRATION_REQ:
      # For testing
      self.process_migration_req(ctrl_msg.msg, addr)
    elif ctrl_msg.msg_type == TYPE_MIGRATION_REPLY:
      self.process_migration_reply(ctrl_msg.msg, addr)
    elif ctrl_msg.msg_type == TYPE_REGISTER_REQ:
      self.process_register_req(ctrl_msg.msg, addr)
    else: 
      print "Unexpected Controller message"

  def process_reset_req(self, msg, addr):
    # Remove existing routes
    for node in self.nodes:
      for node_hash in node.node_hashes:
        self.remove_route_lpm(node_hash.prefix, node_hash.mask, node_hash.prefix_idx)
    self.nodes = {}

    # Add new routes
    nbits = int(math.log(msg.num_nodes, 2))
    mask = 8 * 12 + nbits
    for i in range(msg.num_nodes):
      prefix = [0] * 12
      prefix.append(i << (8 - nbits))
      prefix.extend([0] * 3)
      prefix_idx = self.add_route_lpm(prefix, mask, i)
      start = i << (32 - nbits)
      end = ((i + 1) << (32 - nbits)) - 1
      node = self.Node()
      node.node_hashes.append(self.NodeHash(start=start, end=end, prefix=prefix, mask=mask, prefix_idx=prefix_idx))
      self.nodes[i] = node

    # Reply to server
    ctrl_msg = ControllerMessage()
    ctrl_msg.msg_type = TYPE_RESET_REPLY
    ctrl_msg.msg = ControllerMessage.ResetReply(ack=ACK_OK)
    self.send_msg(ctrl_msg, addr)

  def process_migration_req(self, msg, addr):
    # For testing
    for node in self.nodes.values():
      for node_hash in node.node_hashes:
        if msg.start >= node_hash.start and msg.end <= node_hash.end:
          ctrl_msg = ControllerMessage()
          ctrl_msg.msg_type = TYPE_MIGRATION_REQ
          ctrl_msg.msg = msg
          self.send_msg(ctrl_msg, node.addr)
          return

  def process_migration_reply(self, msg, addr):
    pass

  def process_register_req(self, msg, addr):
    if msg.node_id not in self.nodes:
      print "Node id", msg.node_id, "out of range"
      return

    self.nodes[msg.node_id].addr = addr
    start = self.nodes[msg.node_id].node_hashes[0].start
    end = self.nodes[msg.node_id].node_hashes[0].end

    ctrl_msg = ControllerMessage()
    ctrl_msg.msg_type = TYPE_REGISTER_REPLY
    ctrl_msg.msg = ControllerMessage.RegisterReply(start=start, end=end)
    self.send_msg(ctrl_msg, addr)

  def parse_msg(self, msg, ctrl_msg):
    if len(msg) < PACKET_BASE_SIZE:
      return False
    index = 0
    identifier = struct.unpack('<I', msg[index:index+IDENTIFIER_SIZE])[0]
    if identifier != IDENTIFIER:
      return False
    index += IDENTIFIER_SIZE
    msg_type = struct.unpack('<B', msg[index:index+TYPE_SIZE])[0]
    index += TYPE_SIZE

    ctrl_msg.msg_type = msg_type
    if msg_type == TYPE_RESET_REQ:
      if len(msg) < RESET_REQ_SIZE:
        return False
      num_nodes = struct.unpack('<I', msg[index:index+NUM_NODES_SIZE])[0]
      ctrl_msg.msg = ControllerMessage.ResetRequest(num_nodes=num_nodes)
    elif msg_type == TYPE_RESET_REPLY:
      if len(msg) < REPLY_REPLY_SIZE:
        return False
      ack = struct.unpack('<B', msg[index:index+ACK_SIZE])[0]
      ctrl_msg.msg = ControllerMessage.ResetReply(ack=ack)
    elif msg_type == TYPE_MIGRATION_REQ:
      if len(msg) < MIGRATION_REQ_SIZE:
        return False
      start = struct.unpack('<I', msg[index:index+KEYHASH_SIZE])[0]
      index += KEYHASH_SIZE
      end = struct.unpack('<I', msg[index:index+KEYHASH_SIZE])[0]
      index += KEYHASH_SIZE
      dst_node_id = struct.unpack('<I', msg[index:index+NODE_ID_SIZE])[0]
      ctrl_msg.msg = ControllerMessage.MigrationRequest(start=start, end=end, dst_node_id=dst_node_id)
    elif msg_type == TYPE_MIGRATION_REPLY:
      if len(msg) < MIGRATION_REPLY_SIZE:
        return False
      ack = struct.unpack('<B', msg[index:index+ACK_SIZE])[0]
      ctrl_msg.msg = ControllerMessage.MigrationReply(ack=ack)
    elif msg_type == TYPE_REGISTER_REQ:
      if len(msg) < REGISTER_REQ_SIZE:
        return False
      node_id = struct.unpack('<I', msg[index:index+NODE_ID_SIZE])[0]
      ctrl_msg.msg = ControllerMessage.RegisterRequest(node_id=node_id)
    elif msg_type == TYPE_REGISTER_REPLY:
      if len(msg) < REGISTER_REPLY_SIZE:
        return False
      start = struct.unpack('<I', msg[index:index+KEYHASH_SIZE])[0]
      index += KEYHASH_SIZE
      end = struct.unpack('<I', msg[index:index+KEYHASH_SIZE])[0]
      index += KEYHASH_SIZE
      ctrl_msg.msg = ControllerMessage.RegisterReply(start=start, end=end)
    else:
      return False

    return True

  def send_msg(self, ctrl_msg, addr):
    msg = ''
    msg += struct.pack('<I', IDENTIFIER)
    msg += struct.pack('<B', ctrl_msg.msg_type)
    if ctrl_msg.msg_type == TYPE_RESET_REPLY:
      msg += struct.pack('<B', ctrl_msg.msg.ack)
    elif ctrl_msg.msg_type == TYPE_MIGRATION_REQ:
      msg += struct.pack('<I', ctrl_msg.msg.start)
      msg += struct.pack('<I', ctrl_msg.msg.end)
      msg += struct.pack('<I', ctrl_msg.msg.dst_node_id)
    elif ctrl_msg.msg_type == TYPE_REGISTER_REPLY:
      msg += struct.pack('<I', ctrl_msg.msg.start)
      msg += struct.pack('<I', ctrl_msg.msg.end)
    else:
      print "Wrong controller message type:", ctrl_msg.msg_type
      return
          
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


if __name__ == '__main__':
  controller = PegasusController()
  controller.start()
  controller.join()
