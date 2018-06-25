#!/usr/bin/env python
import sys
import struct
import os

from scapy.all import sniff, sendp, hexdump, get_if_list, get_if_hwaddr
from scapy.all import Packet, IPOption
from scapy.all import ShortField, IntField, LongField, BitField, FieldListField, FieldLenField
from scapy.all import IP, TCP, UDP, Raw
from scapy.layers.inet import _IPOption_HDR

def handle_pkt(pkt):
    if UDP in pkt and pkt[UDP].dport == 12345:
        print "got a packet"
        pkt.show2()
        sys.stdout.flush()

def main():
    if len(sys.argv) < 2:
        print 'pass 1 argument: <interface>'
        exit(1)

    server_iface = sys.argv[1]
    print "sniffing on %s" % server_iface
    sys.stdout.flush()
    sniff(iface = server_iface,
          prn = lambda x: handle_pkt(x))

if __name__ == '__main__':
    main()
