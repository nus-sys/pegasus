#!/usr/bin/env python2
import argparse
import os
from time import sleep

import p4runtime_lib.bmv2
import p4runtime_lib.helper

mac_table = {"00:00:00:00:00:01" : 1,
             "00:00:00:00:00:02" : 2,
             "00:00:00:00:00:03" : 3,
             "00:00:00:00:00:04" : 4,
             "00:00:00:00:00:05" : 5,
             }

rkeys_table = {16 : 0,
               32 : 1}
rkey_forward_table = {0 : ("00:00:00:00:00:01", "10.0.0.1", 1),
                      1 : ("00:00:00:00:00:02", "10.0.0.2", 2),
                      2 : ("00:00:00:00:00:03", "10.0.0.3", 3),
                      3 : ("00:00:00:00:00:04", "10.0.0.4", 4)}

def writeL2ForwardingRules(p4info_helper, sw):
    for (addr, port) in mac_table.items():
        table_entry = p4info_helper.buildTableEntry(
            table_name="MyIngress.tab_mac",
            match_fields={
                "hdr.ethernet.dstAddr": addr
            },
            action_name="MyIngress.l2_forward",
            action_params={
                "port": port,
            })
        sw.WriteTableEntry(table_entry)
    print "Installed l2 forwarding rules on %s" % sw.name


def writeRKeyForwardingRules(p4info_helper, sw):
    for (keyhash, index) in rkeys_table.items():
        table_entry = p4info_helper.buildTableEntry(
            table_name="MyIngress.tab_replicated_keys",
            match_fields={
                "hdr.pegasus.keyhash": keyhash
            },
            action_name="MyIngress.lookup_replicated_key",
            action_params={
                "index": index
            })
        sw.WriteTableEntry(table_entry)
    for (node, (macAddr, ipAddr, port)) in rkey_forward_table.items():
        table_entry = p4info_helper.buildTableEntry(
            table_name="MyIngress.tab_rkey_forward",
            match_fields={
                "meta.dstNode": node
            },
            action_name="MyIngress.rkey_forward",
            action_params={
                "macAddr": macAddr,
                "ip4Addr": ipAddr,
                "port": port
            })
        sw.WriteTableEntry(table_entry)


def readTableRules(p4info_helper, sw):
    '''
    Reads the table entries from all tables on the switch.

    :param p4info_helper: the P4Info helper
    :param sw: the switch connection
    '''
    print '\n----- Reading tables rules for %s -----' % sw.name
    for response in sw.ReadTableEntries():
        for entity in response.entities:
            entry = entity.table_entry
            print "Table name:", p4info_helper.get_tables_name(entry.table_id)
            print entry
            print '-----'


def main(p4info_file_path, bmv2_file_path):
    # Instantiate a P4 Runtime helper from the p4info file
    p4info_helper = p4runtime_lib.helper.P4InfoHelper(p4info_file_path)

    # Create a switch connection object for switch s1;
    # this is backed by a P4 Runtime gRPC connection
    s1 = p4runtime_lib.bmv2.Bmv2SwitchConnection('s1',
                                                 address='127.0.0.1:50051',
                                                 device_id=0)

    # Install the P4 program on the switch
    s1.SetForwardingPipelineConfig(p4info=p4info_helper.p4info,
                                   bmv2_json_file_path=bmv2_file_path)
    print "Installed P4 Program using SetForwardingPipelineConfig on %s" % s1.name

    # Write hard-coded L2 forwarding rules
    writeL2ForwardingRules(p4info_helper, s1)
    writeRKeyForwardingRules(p4info_helper, s1)

    # Read table entries
    readTableRules(p4info_helper, s1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='P4Runtime Controller')
    parser.add_argument('--p4info', help='p4info proto in text format from p4c',
                        type=str, action="store", required=False,
                        default='./build/pegasus.p4info')
    parser.add_argument('--bmv2-json', help='BMv2 JSON file from p4c',
                        type=str, action="store", required=False,
                        default='./build/pegasus.json')
    args = parser.parse_args()

    if not os.path.exists(args.p4info):
        parser.print_help()
        print "\np4info file not found: %s\nHave you run 'make'?" % args.p4info
        parser.exit(1)
    if not os.path.exists(args.bmv2_json):
        parser.print_help()
        print "\nBMv2 JSON file not found: %s\nHave you run 'make'?" % args.bmv2_json
        parser.exit(1)

    main(args.p4info, args.bmv2_json)
