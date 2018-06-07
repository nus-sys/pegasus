#!/usr/bin/env python
import argparse
import sys
import socket
import random
import struct

from scapy.all import sendp, send, get_if_list, get_if_hwaddr
from scapy.all import Packet
from scapy.all import Ether, IP, UDP, TCP
from scapy.all import ByteField, ShortField, IntField, BitField

port0_iface = "veth0"
port1_iface = "veth2"
server_ip = socket.gethostbyname("10.0.0.1")
client_ip = socket.gethostbyname("10.0.0.2")

def main():
    if len(sys.argv) < 2:
        print 'pass 1 arguments: "<message>"'
        exit(1)

    msg = sys.argv[1]

    print "sending on interface %s mac %s to mac %s" % (port0_iface, get_if_hwaddr(port0_iface), get_if_hwaddr(port1_iface))
    pkt = Ether(src=get_if_hwaddr(port0_iface), dst=get_if_hwaddr(port1_iface))
    pkt = pkt / msg
    pkt.show2()
    sendp(pkt, iface=port0_iface, verbose=False)


if __name__ == '__main__':
    main()
