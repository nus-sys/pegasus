# Pegasus Artifact Evaluation

## Macros used in this README

`$REPO`: path to the root of the Pegasus repository

`$SDE`: path to Barefoot SDE

`$SDE_INSTALL`: path to Barefoot SDE installation directory

## Dependencies

* libevent (libevent-dev)
* Intel TBB (libtbb-dev)
* libnuma (libnuma-dev)
* zlib (zlib1g-dev)
* DPDK (tested with version 19.11.1)
* Mellanox OFED (only if using Mellanox NICs, tested with version 5.0-2.1.8.0)
* Barefoot SDE (tested with version 9.1.1, enable Thrift when building SDE)
* Python Sorted Containers (pip install sortedcontainers)
* Python PyREM (pip install pyrem)

## Build

### End-host code

Run `make` in `$REPO`

Note: if you are using Mellanox NICs, you need to modify the following line in `$REPO/emulation/Makefile`
```
HAS_MLX5 := n
```
to
```
HAS_MLX5 := y
```

### P4 switch code

On the target P4 switch:

```
cd $SDE/pkgsrc/p4-build
./configure P4_PATH=$REPO/p4/p4_tofino/pegasus.p4 P4_NAME=pegasus P4_PREFIX=pegasus P4_VERSION=p4-14 P4_FLAGS="--verbose 2" --with-tofino --prefix=$SDE_INSTALL --enable-thrift
make
make install
./configure P4_PATH=$REPO/p4/netcache/one.p4 P4_NAME=netcache P4_PREFIX=netcache P4_VERSION=p4-14 P4_FLAGS="--verbose 2" --with-tofino --prefix=$SDE_INSTALL --enable-thrift
make
make install
```
Note that the location of `p4-build` may depend on the Barefoot SDE version.

## Run

### P4 switch

First, start the Pegasus switch daemon (on the P4 switch):
```
cd $SDE
./run_switchd.sh -p pegasus
```
Or if you are running NetCache, run the following
```
cd $SDE
./run_switchd.sh -p netcache
```
Next, in the switch shell, add and enable all the ports used by the experiment:
```
ucli
pm
port-add 1/- 25G none
port-enb 1/-
...
```
You need to use the right port number (1/-), port speed (25G), and FEC mode (none) based on the switch/cable configurations.

Second, modify JSON files `$REPO/artifact_eval/pegasus.json` and `$REPO/artifact_eval/netcache.json` with the testbed cluster configuration. Specifically, `tab_l2_forward` contains the MAC address of *all* the machines in the testbed (both clients and servers), and which P4 switch ports they are connected to. Add more entries if needed. `tab_node_forward` (only in pegasus.json) contains the MAC, IP, UDP port, and the connected P4 switch port of *only* the servers (don't add client machines here). These entries are numerically ordered (0, 1, 2, 3, ...), and they have to follow the same order as the `node` entries in `$REPO/artifact_eval/testbed.config`. Similarly, add more entries if needed.

Third, start the Pegasus switch controller:
```
cd $REPO
./artifact_eval/run_pegasus_controller.sh
```
Or if you are running NetCache, run the following
```
cd $REPO
./artifact_eval/run_netcache_controller.sh
```

### Ehd-host

First, modify configuration file `$REPO/artifact_eval/testbed.config`. The file has the following entries:
1. Servers:
```
node mac|ip|port|dev_port[|blacklist]
```
* mac: server MAC address
* ip: server IP address
* port: server UDP port
* dev_port: for NICs with multiple ports, this is the port number (not used by the experiments, so keep 0 here)
* blacklist: optional. If there are multiple NICS, and/or if the DPDK NIC has multiple ports, list all the PCI slot numbers (bus:device.function) that are *not* used by the experiments.
Note that the `node` entries are ordered. Make sure their order matches the numerical order of `tab_node_forward` in `$REPO/artifact_eval/pegasus.json`.
