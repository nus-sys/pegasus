#!/usr/bin/env python
import sys
import struct
import os

from scapy.all import sniff, sendp, hexdump, get_if_list, get_if_hwaddr
from scapy.all import Packet, IPOption
from scapy.all import ShortField, IntField, LongField, BitField, FieldListField, FieldLenField
from scapy.all import IP, TCP, UDP, Raw
from scapy.layers.inet import _IPOption_HDR

server_iface = "veth0"

def handle_pkt(pkt):
    if UDP in pkt and pkt[UDP].dport == 12345:
        print "got a packet"
        pkt.show2()
        sys.stdout.flush()

def main():
    print "sniffing on %s" % server_iface
    sys.stdout.flush()
    sniff(iface = server_iface,
          prn = lambda x: handle_pkt(x))

if __name__ == '__main__':
    main()
