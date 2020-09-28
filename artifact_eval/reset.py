#!/usr/bin/env python

import sys

from scapy.all import send
from scapy.all import Packet
from scapy.all import Ether, IP, UDP, TCP
from scapy.all import ByteField, LEShortField

CTRL_IP = "198.19.3.16"
CTRL_UDP = 12345

class ResetPacket(Packet):
    name = "Reset"
    fields_desc = [LEShortField("id", 0xDEAC),
                  ByteField("type", 0x0),
                  LEShortField("nodes", None),
                  LEShortField("rkeys", None)]

def main():
    if len(sys.argv) < 3:
        print "pass 2 arguments: <num_nodes> <num_rkeys>"
        exit(1)

    nodes = int(sys.argv[1])
    rkeys = int(sys.argv[2])

    pkt = IP(dst=CTRL_IP)/UDP(dport=CTRL_UDP)/ResetPacket(nodes=nodes, rkeys=rkeys)
    send(pkt, verbose=False)

if __name__ == '__main__':
    main()
