######################*license start*###################################
#   Copyright (c) 2003-2008 Cavium Inc. (support@cavium.com). All rights
#   reserved.
#
#
#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the following conditions are
#   met:
#
#       * Redistributions of source code must retain the above copyright
#         notice, this list of conditions and the following disclaimer.
#
#       * Redistributions in binary form must reproduce the above
#         copyright notice, this list of conditions and the following
#         disclaimer in the documentation and/or other materials provided
#         with the distribution.
#
#       * Neither the name of Cavium Inc. nor the names of
#         its contributors may be used to endorse or promote products
#         derived from this software without specific prior written
#         permission.
#
#   TO THE MAXIMUM EXTENT PERMITTED BY LAW, THE SOFTWARE IS PROVIDED "AS IS"
#   AND WITH ALL FAULTS AND CAVIUM NETWORKS MAKES NO PROMISES, REPRESENTATIONS
#   OR WARRANTIES, EITHER EXPRESS, IMPLIED, STATUTORY, OR OTHERWISE, WITH
#   RESPECT TO THE SOFTWARE, INCLUDING ITS CONDITION, ITS CONFORMITY TO ANY
#   REPRESENTATION OR DESCRIPTION, OR THE EXISTENCE OF ANY LATENT OR PATENT
#   DEFECTS, AND CAVIUM SPECIFICALLY DISCLAIMS ALL IMPLIED (IF ANY) WARRANTIES
#   OF TITLE, MERCHANTABILITY, NONINFRINGEMENT, FITNESS FOR A PARTICULAR
#   PURPOSE, LACK OF VIRUSES, ACCURACY OR COMPLETENESS, QUIET ENJOYMENT, QUIET
#   POSSESSION OR CORRESPONDENCE TO DESCRIPTION.  THE ENTIRE RISK ARISING OUT
#   OF USE OR PERFORMANCE OF THE SOFTWARE LIES WITH YOU.
#
#
#   For any questions regarding licensing please contact marketing@cavium.com
#

#
# File version info: $Id: Makefile 69865 2012-02-11 02:16:39Z cchavva $


ifndef OCTEON_ROOT
OCTEON_ROOT = ../..
endif

MODEL=${OCTEON_MODEL}


# Default simulation parameters
GMX_TO_USE?="gmx0"
NUM_PROCESSORS=4
INPUT_FILES=-i input-0.data -i input-1.data -i input-2.data -i input-3.data
DIFF_FILES=diff input-0.data output-0.data && diff input-1.data output-1.data && diff input-2.data output-2.data && diff input-3.data output-3.data
DATA_FILE=input-0.data
NUM_CORES=16


SEL_VZ_INTERVAL=-noperf=application_main_loop -wait=application_main_loop -quiet

TMP=${findstring npi, ${GMX_TO_USE}}
ifneq ($(TMP),)
    INPUT_FILES=-i input-32.data -i input-33.data -i input-34.data  -i input-35.data
    DIFF_FILES=diff input-32.data output-32.data && diff input-33.data output-33.data && diff input-34.data output-34.data && diff input-35.data output-35.data
    DATA_FILE=input-32.data
endif

TMP=${findstring OCTEON_CN3020,${MODEL}}
ifeq ($(TMP),)
TMP=${findstring OCTEON_CN31XX,${MODEL}}
endif
ifeq ($(TMP),)
TMP=${findstring OCTEON_CN50XX,${MODEL}}
endif
ifneq ($(TMP),)
    NUM_PROCESSORS=2
    INPUT_FILES=-i input-0.data -i input-1.data -i input-2.data
    DIFF_FILES=diff input-0.data output-0.data && diff input-1.data output-1.data && diff input-2.data output-2.data
    NUM_CORES=$(NUM_PROCESSORS)
endif

TMP=${findstring OCTEON_CN3010,${MODEL}}
ifneq ($(TMP),)
    NUM_PROCESSORS=1
    INPUT_FILES=-i input-0.data -i input-1.data -i input-2.data
    DIFF_FILES=diff input-0.data output-0.data && diff input-1.data output-1.data && diff input-2.data output-2.data
    NUM_CORES=$(NUM_PROCESSORS)
endif

TMP=${findstring OCTEON_CN3005,${MODEL}}
ifneq ($(TMP),)
    NUM_PROCESSORS=1
    INPUT_FILES=-i input-0.data -i input-1.data
    DIFF_FILES=diff input-0.data output-0.data && diff input-1.data output-1.data
    NUM_CORES=$(NUM_PROCESSORS)
endif
TMP=${findstring OCTEON_CN52XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=4
endif
TMP=${findstring OCTEON_CN56XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=12
endif
TMP=${findstring OCTEON_CN61XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=4
    GMX=${findstring gmx1, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-16.data -i input-17.data -i input-18.data -i input-19.data
        DIFF_FILES=diff input-16.data output-16.data && diff input-17.data output-17.data && diff input-18.data output-18.data && diff input-19.data output-19.data
    endif
endif
TMP=${findstring OCTEON_CN63XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=6
endif
TMP=${findstring OCTEON_CN66XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=10
    GMX=${findstring gmx1, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-16.data -i input-17.data -i input-18.data -i input-19.data
        DIFF_FILES=diff input-16.data output-16.data && diff input-17.data output-17.data && diff input-18.data output-18.data && diff input-19.data output-19.data
    endif
endif
TMP=${findstring OCTEON_CN68XX,${MODEL}}
ifneq ($(TMP),)
    NUM_CORES=32
    QLM_LANE_MASK=#-qlm_is_disabled_mask=6  # to disable ilk
    QLM_LANE_MASK=#-qlm_is_ilk_mask=6       # to enable ilk
    INPUT_FILES=-i input-2048.data -i input-2064.data -i input-2080.data -i input-2096.data
    DIFF_FILES=diff input-2048.data output-2048.data && diff input-2064.data output-2064.data && diff input-2080.data output-2080.data && diff input-2096.data output-2096.data
    DATA_FILE=input-2048.data
    GMX=${findstring gmx2, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-2560.data -i input-2576.data -i input-2592.data -i input-2608.data
        DIFF_FILES=diff input-2560.data output-2560.data && diff input-2576.data output-2576.data && diff input-2592.data output-2592.data && diff input-2608.data output-2608.data
        DATA_FILE=input-2560.data
        QLM_LANE_MASK=
    endif
    GMX=${findstring gmx3, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-2816.data -i input-2832.data -i input-2848.data -i input-2864.data
        DIFF_FILES=diff input-2816.data output-2816.data && diff input-2832.data output-2832.data && diff input-2848.data output-2848.data && diff input-2864.data output-2864.data
        DATA_FILE=input-2816.data
        QLM_LANE_MASK=
    endif
    GMX=${findstring gmx4, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-3072.data -i input-3088.data -i input-3104.data -i input-3120.data
        DIFF_FILES=diff input-3072.data output-3072.data && diff input-3088.data output-3088.data && diff input-3104.data output-3104.data && diff input-3120.data output-3120.data
        DATA_FILE=input-3072.data
        QLM_LANE_MASK=
    endif
    GMX=${findstring ilk0, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-1024.data
        DIFF_FILES=diff input-1024.data output-1024.data
        DATA_FILE=input-1024.data
        QLM_LANE_MASK=-qlm_is_ilk_mask=6
    endif
    GMX=${findstring ilk1, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-1280.data
        DIFF_FILES=diff input-1280.data output-1280.data
        DATA_FILE=input-1280.data
        QLM_LANE_MASK=-qlm_is_ilk_mask=6
    endif
    GMX=${findstring npi, ${GMX_TO_USE}}
    ifneq ($(GMX),)
        INPUT_FILES=-i input-256.data -i input-257.data -i input-258.data  -i input-259.data
        DIFF_FILES=diff input-256.data output-256.data && diff input-257.data output-257.data && diff input-258.data output-258.data && diff input-259.data output-259.data
        DATA_FILE=input-256.data
        QLM_LANE_MASK=-qlm_is_sgmii_mask=0x1
    endif
endif

TMP=${findstring OCTEON_CNF71XX,${MODEL}}
ifneq ($(TMP),)
    INPUT_FILES=-i input-0.data -i input-1.data
    DIFF_FILES=diff input-0.data output-0.data && diff input-1.data output-1.data
endif

ifndef OCTEON_SIM_PACKET_TCP_PORT
OCTEON_SIM_PACKET_TCP_PORT=2000
endif

all: application-target $(DATA_FILE)

#  standard common Makefile fragment

include $(OCTEON_ROOT)/common.mk

#  include relevant component Makefile fragments

dir := $(OCTEON_ROOT)/executive
include $(dir)/cvmx.mk

#  application specification

ifeq ($(hotplug),yes)
	TARGET = router_hotplug$(PREFIX)
else
	TARGET = router$(PREFIX)
endif

OBJS = $(OBJ_DIR)/router.o $(OBJ_DIR)/pegasus.o $(OBJ_DIR)/city_hash.o $(OBJ_DIR)/hash_table.o
DEPS = $(OBJS:%.o=%.d)


CFLAGS_LOCAL = -g -O2 -W -Wall -Wno-unused-parameter -G0
ifeq ($(hotplug),yes)
	CFLAGS_LOCAL += -DENABLE_PASSTHROUGH_HOTPLUG
endif

include $(OCTEON_ROOT)/application.mk

$(DATA_FILE): build_data.sh
	./build_data.sh ${GMX_TO_USE}

clean:
	rm -f $(TARGET) output.log input-*.data output-*.data run-all.log
	rm -f $(CLEAN_LIST) $(OBJS) $(DEPS)
	rm -f $(CVMX_CONFIG)

clobber: clean
	rm -rf $(OBJ_DIR)

run: $(TARGET) $(DATA_FILE)
	#
	# Start oct-packet-io to pipe packet in and out of the simulator. It
	# will terminate automatically when the simulator terminates. Input
	# is taken from input-[0-3].data. Output is logged to output-[0-3].data.
	# The option "-c 1" is used to force the output cycle count to match
	# the input. This allows a simple diff to look for errors.
	#
	oct-packet-io -w 5 -p ${OCTEON_SIM_PACKET_TCP_PORT} -c 1 -o output-%d.data ${INPUT_FILES} &
	#
	# Run the simulation. All outout will go in output.log but console
	# messages will also display to the user.
	#
	oct-sim $(TARGET) -quiet -serve=${OCTEON_SIM_PACKET_TCP_PORT} -maxcycles=120000000 -numcores=${NUM_PROCESSORS} ${PASSTHROUGH_SIM_MODES} ${QLM_LANE_MASK}
	#
	# Diff the output with the original input. They should match
	#
	if ${DIFF_FILES}; \
	then \
		echo "Diffing packets - TEST PASSED"; \
	else \
		echo "Diffing packets - TEST FAILED" && false; \
	fi

run_vz: $(TARGET) $(DATA_FILE)
	oct-packet-io -w 5 -p ${OCTEON_SIM_PACKET_TCP_PORT} -c 1 -o output-%d.data ${INPUT_FILES} &
	oct-sim $(TARGET) -vz ${SEL_VZ_INTERVAL} -serve=${OCTEON_SIM_PACKET_TCP_PORT} -maxcycles=120000000 -numcores=${NUM_PROCESSORS} ${PASSTHROUGH_SIM_MODES} ${QLM_LANE_MASK}
	if ${DIFF_FILES}; \
	then \
		echo "Diffing packets - TEST PASSED"; \
	else \
		echo "Diffing packets - TEST FAILED" && false; \
	fi

run-linux: $(DATA_FILE)
	# Please see README-Linux.txt for addition steps needed to
	# run this example under Linux
	oct-packet-io -w 5 -p ${OCTEON_SIM_PACKET_TCP_PORT} -c 1 -o output-%d.data ${INPUT_FILES} &
	cd $(OCTEON_ROOT)/linux/kernel_2.6; ./oct-linux -quiet -numcores=$(NUM_PROCESSORS)


run-all:
	for ((cores=1; cores<=$(NUM_CORES); cores++)); do \
		make -s run NUM_PROCESSORS=$${cores}; \
	done | tee run-all.log | grep "cycles/packet"


.PHONY: flash
flash: ${TARGET}
	mount /mnt/cf1
	mipsisa64-octeon-elf-strip -o /mnt/cf1/${TARGET} ${TARGET}
	umount /mnt/cf1
