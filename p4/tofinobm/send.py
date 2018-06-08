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

server_mac = "FF:FF:FF:FF:FF:FF"
client_mac = "00:00:00:00:00:09"
client_iface = "veth8"
server_ip = socket.gethostbyname("10.0.0.255")
client_ip = socket.gethostbyname("10.0.0.9")

class Pegasus(Packet):
    name = "PEGASUS"
    fields_desc = [ShortField("id", 0x5047),
                   ByteField("op", None),
                   IntField("keyhash", None),
                   ByteField("node", 0),
                   ShortField("load", 0)]

def main():
    if len(sys.argv) < 4:
        print 'pass 3 arguments: <op> <keyhash> "<message>"'
        exit(1)

    op = int(sys.argv[1])
    keyhash = int(sys.argv[2])
    msg = sys.argv[3]
    dst_ip = server_ip
    src_ip = client_ip

    pkt = Ether(src=client_mac, dst=server_mac)
    pkt = pkt / IP(dst=dst_ip, src=src_ip)
    pkt = pkt / UDP(dport=12345, sport=random.randint(49152,65535))
    pkt = pkt / Pegasus(op=op, keyhash=keyhash)
    pkt = pkt / msg
    pkt.show2()
    sendp(pkt, iface=client_iface, verbose=False)


if __name__ == '__main__':
    main()
