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

mac_addrs = {"10.0.0.1" : "00:00:00:00:00:01",
             "10.0.0.2" : "00:00:00:00:00:02",
             "10.0.0.3" : "00:00:00:00:00:03",
             "10.0.0.4" : "00:00:00:00:00:04",
             "10.0.0.5" : "00:00:00:00:00:05"
             }

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

    if len(sys.argv)<5:
        print 'pass 4 arguments: <destination> <op> <keyhash> "<message>"'
        exit(1)

    addr = socket.gethostbyname(sys.argv[1])
    op = int(sys.argv[2])
    keyhash = int(sys.argv[3])
    iface = get_if()

    print "sending on interface %s to %s" % (iface, str(addr))
    pkt =  Ether(src=get_if_hwaddr(iface), dst=mac_addrs[str(addr)])
    pkt = pkt /IP(dst=addr) / UDP(dport=12345, sport=random.randint(49152,65535)) / Pegasus(op=op, keyhash=keyhash) / sys.argv[4]
    pkt.show2()
    sendp(pkt, iface=iface, verbose=False)


if __name__ == '__main__':
    main()
