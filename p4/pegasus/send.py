#!/usr/bin/env python
import argparse
import sys
import socket
import random
import struct

from scapy.all import sendp, send, get_if_list, get_if_hwaddr
from scapy.all import Packet
from scapy.all import Ether, IP, UDP, TCP
from scapy.all import ByteField, ShortField, IntField

dst_ip = socket.gethostbyname("10.0.0.255")
dst_mac = "ff:ff:ff:ff:ff:ff"

class Pegasus(Packet):
    name = "PEGASUS"
    fields_desc = [ShortField("id", 0x5047),
                   ByteField("op", None),
                   IntField("keyhash", None),
                   ShortField("load", 0)]

def get_if():
    ifs=get_if_list()
    iface=None # "h1-eth0"
    for i in get_if_list():
        if "eth0" in i:
            iface=i
            break;
    if not iface:
        print "Cannot find eth0 interface"
        exit(1)
    return iface

def main():

    if len(sys.argv) < 4:
        print 'pass 3 arguments: <op> <keyhash> "<message>"'
        exit(1)

    op = int(sys.argv[1])
    keyhash = int(sys.argv[2])
    msg = sys.argv[3]
    iface = get_if()

    print "sending on interface %s to %s" % (iface, str(dst_ip))
    pkt = Ether(src=get_if_hwaddr(iface), dst=dst_mac)
    pkt = pkt /IP(dst=dst_ip) / UDP(dport=12345, sport=random.randint(49152,65535)) / Pegasus(op=op, keyhash=keyhash) / msg
    pkt.show2()
    sendp(pkt, iface=iface, verbose=False)


if __name__ == '__main__':
    main()
