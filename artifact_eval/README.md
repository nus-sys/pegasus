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
* Mellanox OFED (only if using Mellanox NICs)
* Barefoot SDE (tested with version 9.1.1)
* Python PyREM (pip install pyrem)

## Build

### End-host code

Run `make` in `$REPO`

### P4 switch code

On the target P4 switch:

```
cd $SDE/pkgsrc/p4-build
./configure P4_PATH=$REPO/p4/p4_tofino/pegasus.p4 P4_NAME=pegasus P4_PREFIX=pegasus P4_VERSION=p4-14 P4_FLAGS="--verbose 2" --with-tofino --prefix=$SDE_INSTALL --enable-thrift
make
make install
```

Note that the location of `p4-build` may depend on the Barefoot SDE version.
