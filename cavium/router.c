/***********************license start***************
 * Copyright (c) 2003-2010  Cavium Inc. (support@cavium.com). All rights
 * reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.

 *   * Neither the name of Cavium Inc. nor the names of
 *     its contributors may be used to endorse or promote products
 *     derived from this software without specific prior written
 *     permission.

 * This Software, including technical data, may be subject to U.S. export  control
 * laws, including the U.S. Export Administration Act and its  associated
 * regulations, and may be subject to export or import  regulations in other
 * countries.

 * TO THE MAXIMUM EXTENT PERMITTED BY LAW, THE SOFTWARE IS PROVIDED "AS IS"
 * AND WITH ALL FAULTS AND CAVIUM INC. MAKES NO PROMISES, REPRESENTATIONS OR
 * WARRANTIES, EITHER EXPRESS, IMPLIED, STATUTORY, OR OTHERWISE, WITH RESPECT TO
 * THE SOFTWARE, INCLUDING ITS CONDITION, ITS CONFORMITY TO ANY REPRESENTATION OR
 * DESCRIPTION, OR THE EXISTENCE OF ANY LATENT OR PATENT DEFECTS, AND CAVIUM
 * SPECIFICALLY DISCLAIMS ALL IMPLIED (IF ANY) WARRANTIES OF TITLE,
 * MERCHANTABILITY, NONINFRINGEMENT, FITNESS FOR A PARTICULAR PURPOSE, LACK OF
 * VIRUSES, ACCURACY OR COMPLETENESS, QUIET ENJOYMENT, QUIET POSSESSION OR
 * CORRESPONDENCE TO DESCRIPTION. THE ENTIRE  RISK ARISING OUT OF USE OR
 * PERFORMANCE OF THE SOFTWARE LIES WITH YOU.
 ***********************license end**************************************/


#include <stdio.h>
#include <string.h>

#include "cvmx-config.h"
#include "cvmx.h"
#include "cvmx-spinlock.h"
#include "cvmx-fpa.h"
#include "cvmx-ilk.h"
#include "cvmx-pip.h"
#include "cvmx-ipd.h"
#include "cvmx-pko.h"
#include "cvmx-dfa.h"
#include "cvmx-pow.h"
#include "cvmx-gmx.h"
#include "cvmx-sysinfo.h"
#include "cvmx-coremask.h"
#include "cvmx-bootmem.h"
#include "cvmx-helper.h"
#include "cvmx-app-hotplug.h"
#include "cvmx-helper-cfg.h"
#include "cvmx-srio.h"

#include "pegasus.h"

/*
 * Configure passthrough to run with lockless pko operations.
 */
//#define ENABLE_LOCKLESS_PKO

#define FAU_PACKETS     ((cvmx_fau_reg_64_t)(CVMX_FAU_REG_AVAIL_BASE + 0))   /**< Fetch and add for counting packets processed */
#define FAU_ERRORS      ((cvmx_fau_reg_64_t)(CVMX_FAU_REG_AVAIL_BASE + 8))   /**< Fetch and add for counting detected errors */
#define FAU_OUTSTANDING ((cvmx_fau_reg_64_t)(CVMX_FAU_REG_AVAIL_BASE + 16))  /**< Fetch and add for counting outstanding packets */

static CVMX_SHARED uint64_t start_cycle;
static CVMX_SHARED uint64_t stop_cycle;
static unsigned int packet_termination_num;

static int volatile core_unplug_requested  = 0;
static int volatile app_shutdown_requested = 0;

//#define ENABLE_PASSTHROUGH_HOTPLUG 1

#ifdef ENABLE_PASSTHROUGH_HOTPLUG
static int app_shutdown = 1;
/* Passthrough hotplug callback arguments */
typedef struct passthrough_hp_cb_args {
    void *mem_ptr;
} passthrough_hp_cb_args_t;

void passthrough_shutdown_callback(passthrough_hp_cb_args_t* data)
{
    if (cvmx_coremask_first_core(cvmx_sysinfo_get()->core_mask))
        printf("Hotplug callback called for core #%d data %p\n",
               cvmx_get_core_num(), data->mem_ptr);
    app_shutdown_requested = 1;
}

void passthrough_unplug_callback(passthrough_hp_cb_args_t* data)
{
    if (cvmx_coremask_first_core(cvmx_sysinfo_get()->core_mask))
        printf("Unlpug callback called for core #%d data %p\n",
               cvmx_get_core_num(), data->mem_ptr);
    core_unplug_requested = 1;
}

CVMX_SHARED passthrough_hp_cb_args_t passthrough_hp_cb_args_data;

int init_hotplug(void)
{
    passthrough_hp_cb_args_data.mem_ptr = (void*)0xA5A5A5A5;

    if (is_core_being_hot_plugged())
    {
        printf("core=%d is being hotplugged\n", cvmx_get_core_num());
    }

    if (cvmx_coremask_first_core(cvmx_sysinfo_get()->core_mask) &&
        !is_core_being_hot_plugged())
    {
        cvmx_app_hotplug_callbacks_t cb;
        bzero(&cb, sizeof(cb));
        cb.shutdown_callback =  (void(*)(void*))passthrough_shutdown_callback;
        cb.unplug_core_callback =  (void(*)(void*))passthrough_unplug_callback;
        /* Register application for hotplug. this only needs to be done once */
        cvmx_app_hotplug_register_cb(&cb, &passthrough_hp_cb_args_data, app_shutdown);
    }

    /* Activate hotplug */
    if (cvmx_app_hotplug_activate())
    {
        printf("ERROR: cvmx_hotplug_activate() failed\n");
        return -1;
    }
    return 0;
}

#endif

/* Note: The dump_packet routine that used to be here has been moved to
    cvmx_helper_dump_packet. */
//#define DUMP_PACKETS 1
//#define DUMP_STATS
//#define SWAP_MAC_ADDR

#ifdef SWAP_MAC_ADDR
static inline void
swap_mac_addr(uint64_t pkt_ptr)
{
    uint16_t s;
    uint32_t w;

    /* assuming an IP/IPV6 pkt i.e. L2 header is 2 byte aligned, 4 byte non-aligned */
    s = *(uint16_t*)pkt_ptr;
    w = *(uint32_t*)(pkt_ptr+2);
    *(uint16_t*)pkt_ptr = *(uint16_t*)(pkt_ptr+6);
    *(uint32_t*)(pkt_ptr+2) = *(uint32_t*)(pkt_ptr+8);
    *(uint16_t*)(pkt_ptr+6) = s;
    *(uint32_t*)(pkt_ptr+8) = w;
}
#endif

/**
 * Setup the Cavium Simple Executive Libraries using defaults
 *
 * @param num_packet_buffers
 *               Number of outstanding packets to support
 * @return Zero on success
 */
static int application_init_simple_exec(int num_packet_buffers)
{
    int result;
    int port, interface;

    if (cvmx_helper_initialize_fpa(num_packet_buffers, num_packet_buffers, CVMX_PKO_MAX_OUTPUT_QUEUES * 4, 0, 0))
        return -1;

    if (cvmx_helper_initialize_sso(num_packet_buffers))
        return -1;

    /* Don't enable RED on simulator */
    if (cvmx_sysinfo_get()->board_type != CVMX_BOARD_TYPE_SIM)
        cvmx_helper_setup_red(num_packet_buffers/4, num_packet_buffers/8);

    if (octeon_has_feature(OCTEON_FEATURE_NO_WPTR))
    {
        cvmx_ipd_ctl_status_t ipd_ctl_status;
        printf("Enabling CVMX_IPD_CTL_STATUS[NO_WPTR]\n");
        ipd_ctl_status.u64 = cvmx_read_csr(CVMX_IPD_CTL_STATUS);
        ipd_ctl_status.s.no_wptr = 1;
        cvmx_write_csr(CVMX_IPD_CTL_STATUS, ipd_ctl_status.u64);
    }

    cvmx_helper_cfg_opt_set(CVMX_HELPER_CFG_OPT_USE_DWB, 0);
    result = cvmx_helper_initialize_packet_io_global();

    /* Leave 16 bytes space for the ethernet header */
    cvmx_write_csr(CVMX_PIP_IP_OFFSET, 2);
    /* Enable storing short packets only in the WQE */
    for (interface = 0; interface < cvmx_helper_get_number_of_interfaces(); interface++)
    {
        /* Set the frame max size and jabber size to 65535, as the defaults
           are too small. */
        cvmx_helper_interface_mode_t imode = cvmx_helper_interface_get_mode(interface);
        int num_ports = cvmx_helper_ports_on_interface(interface);

        switch (imode)
        {
            case CVMX_HELPER_INTERFACE_MODE_SGMII:
            case CVMX_HELPER_INTERFACE_MODE_XAUI:
            case CVMX_HELPER_INTERFACE_MODE_RXAUI:
                for (port=0; port < num_ports; port++)
                    cvmx_write_csr(CVMX_GMXX_RXX_JABBER(port,interface), 65535);
                if (octeon_has_feature(OCTEON_FEATURE_PKND))
                {
                    cvmx_pip_prt_cfgx_t pip_prt;
                    cvmx_pip_frm_len_chkx_t pip_frm_len_chkx;
                    pip_frm_len_chkx.u64 = 0;
                    pip_frm_len_chkx.s.minlen = 64;
                    pip_frm_len_chkx.s.maxlen = -1;
                    for (port=0; port<num_ports; port++)
                    {
                       /* Check which PIP_FRM_LEN_CHK register is used for this port-kind
                          for MINERR and MAXERR checks */
                        int pknd = cvmx_helper_get_pknd(interface, port);
                        pip_prt.u64 = cvmx_read_csr(CVMX_PIP_PRT_CFGX(pknd));
                        cvmx_write_csr(CVMX_PIP_FRM_LEN_CHKX(pip_prt.cn68xx.len_chk_sel), pip_frm_len_chkx.u64);
                    }
                }
                else
                {
                    cvmx_pip_frm_len_chkx_t pip_frm_len_chkx;
                    pip_frm_len_chkx.u64 = cvmx_read_csr(CVMX_PIP_FRM_LEN_CHKX(interface));
                    pip_frm_len_chkx.s.minlen = 64;
                    pip_frm_len_chkx.s.maxlen = -1;
                    cvmx_write_csr(CVMX_PIP_FRM_LEN_CHKX(interface), pip_frm_len_chkx.u64);
                }
                break;

            case CVMX_HELPER_INTERFACE_MODE_RGMII:
            case CVMX_HELPER_INTERFACE_MODE_GMII:
                if (OCTEON_IS_MODEL(OCTEON_CN50XX))
                {
                    cvmx_pip_frm_len_chkx_t pip_frm_len_chkx;
                    pip_frm_len_chkx.u64 = cvmx_read_csr(CVMX_PIP_FRM_LEN_CHKX(interface));
                    pip_frm_len_chkx.s.minlen = 64;
                    pip_frm_len_chkx.s.maxlen = -1;
                    cvmx_write_csr(CVMX_PIP_FRM_LEN_CHKX(interface), pip_frm_len_chkx.u64);
                }
                for (port=0; port < num_ports; port++)
                {
                    if (!OCTEON_IS_MODEL(OCTEON_CN50XX))
                        cvmx_write_csr(CVMX_GMXX_RXX_FRM_MAX(port,interface), 65535);
                    cvmx_write_csr(CVMX_GMXX_RXX_JABBER(port,interface), 65535);
                }
                break;
            case CVMX_HELPER_INTERFACE_MODE_ILK:
                for (port=0; port < num_ports; port++)
                {
                    int ipd_port = cvmx_helper_get_ipd_port(interface, port);
                    cvmx_ilk_enable_la_header(ipd_port, 1);
                }
                break;
            default:
                break;
        }

        for (port=0; port < num_ports; port++)
        {
            cvmx_pip_port_cfg_t port_cfg;
            int pknd = port;
            if (octeon_has_feature(OCTEON_FEATURE_PKND))
                pknd = cvmx_helper_get_pknd(interface, port);
            else
                pknd = cvmx_helper_get_ipd_port(interface, port);
            port_cfg.u64 = cvmx_read_csr(CVMX_PIP_PRT_CFGX(pknd));
            port_cfg.s.dyn_rs = 1;
            cvmx_write_csr(CVMX_PIP_PRT_CFGX(pknd), port_cfg.u64);
        }
    }


    /* Initialize the FAU registers. */
    cvmx_fau_atomic_write64(FAU_ERRORS, 0);
    if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
    {
        cvmx_fau_atomic_write64(FAU_PACKETS, 0);
        cvmx_fau_atomic_write64(FAU_OUTSTANDING, 0);
    }

    return result;
}

/**
 * Clean up and properly shutdown the simple exec libraries.
 *
 * @return Zero on success. Non zero means some resources are
 *         unaccounted for. In this case error messages will have
 *         been displayed during shutdown.
 */
static int application_shutdown_simple_exec(void)
{
    int result = 0;
    int status;
    int pool;

    cvmx_helper_shutdown_packet_io_global();

    for (pool=0; pool<CVMX_FPA_NUM_POOLS; pool++)
    {
        if (cvmx_fpa_get_block_size(pool) > 0)
        {
            status = cvmx_fpa_shutdown_pool(pool);
            result |= status;
        }
    }

    return result;
}

/**
 * Process incoming packets. Just send them back out the
 * same interface.
 *
 */
void application_main_loop(void)
{
    cvmx_wqe_t *    work;
    uint64_t        port;
    cvmx_buf_ptr_t  packet_ptr;
    cvmx_pko_command_word0_t pko_command;
    const int use_ipd_no_wptr = octeon_has_feature(OCTEON_FEATURE_NO_WPTR);
    int holds_atomic_tag = 0;
    int queue, ret, pko_port, corenum;

    pko_port = -1;
    corenum = cvmx_get_core_num();

    /* Build a PKO pointer to this packet */
    pko_command.u64 = 0;
    if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
    {
       pko_command.s.size0 = CVMX_FAU_OP_SIZE_64;
       pko_command.s.subone0 = 1;
       pko_command.s.reg0 = FAU_OUTSTANDING;
    }

    /* Pegasus initialization */
    pegasus_init();

    while (1)
    {

#ifdef ENABLE_PASSTHROUGH_HOTPLUG
        if ((core_unplug_requested || app_shutdown_requested) && (app_shutdown))
        {
            printf("core=%d : is unplugged\n",cvmx_get_core_num());
            if (holds_atomic_tag) cvmx_pow_tag_sw_null_nocheck();
            break;
        }
#endif
        /* get the next packet/work to process from the POW unit. */
        if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
        {
           work = cvmx_pow_work_request_sync(CVMX_POW_NO_WAIT);
           if (work == NULL) {
               if (cvmx_fau_fetch_and_add64(FAU_PACKETS, 0) == packet_termination_num)
                   break;
               continue;
           }
        }
        else
        {
           work = cvmx_pow_work_request_sync(CVMX_POW_WAIT);
           if (work == NULL) {
               continue;
           }
        }


        port = cvmx_wqe_get_port(work);

#if defined(CVMX_HELPER_ILK_LA_MODE_INTERFACE0) || defined(CVMX_HELPER_ILK_LA_MODE_INTERFACE1)
        /* Interlaken - fix port number */
        if (((port & 0xffe) == 0x480) || ((port & 0xffe) == 0x580))
            port &= ~0x80;
#endif

        /* Check for errored packets, and drop.  If sender does not respond
        ** to backpressure or backpressure is not sent, packets may be truncated if
        ** the GMX fifo overflows */
        if (cvmx_unlikely(work->word2.snoip.rcv_error))
        {
            /* Work has error, so drop */
            cvmx_helper_free_packet_data(work);
            if (use_ipd_no_wptr)
                cvmx_fpa_free(work, CVMX_FPA_PACKET_POOL, 0);
            else
                cvmx_fpa_free(work, CVMX_FPA_WQE_POOL, 0);
            continue;
        }

        /*
         * Insert packet processing here.
         *
         * Define DUMP_PACKETS to dump packets to the console.
         * Note that due to multiple cores executing in parallel, the output
         * will likely be interleaved.
         *
         */
        #ifdef DUMP_PACKETS
        printf("Processing packet\n");
        cvmx_helper_dump_packet(work);
        #endif

#ifdef DUMP_STATS
        printf ("port to send out: %lu\n", port);
        cvmx_helper_show_stats(port);
#endif


#ifdef ENABLE_LOCKLESS_PKO
        queue = cvmx_pko_get_base_queue_per_core(port, cvmx_get_core_num());
        cvmx_pko_send_packet_prepare(port, queue, CVMX_PKO_LOCK_NONE);
#else
        /*
         * Begin packet output by requesting a tag switch to atomic.
         * Writing to a packet output queue must be synchronized across cores.
         */
	if (octeon_has_feature(OCTEON_FEATURE_PKND))
	{
	    /* PKO internal port is different than IPD port */
	    pko_port = cvmx_helper_cfg_ipd2pko_port_base(port);
	    queue = cvmx_pko_get_base_queue_pkoid(pko_port);
	    queue += (corenum % cvmx_pko_get_num_queues_pkoid(pko_port));
	}
	else
	{
	    queue = cvmx_pko_get_base_queue(port);
	    queue += (corenum % cvmx_pko_get_num_queues(port));
	}
        cvmx_pko_send_packet_prepare(port, queue, CVMX_PKO_LOCK_ATOMIC_TAG);
#endif

        if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
        {
           /* Increment the total packet counts */
           cvmx_fau_atomic_add64(FAU_PACKETS, 1);
           cvmx_fau_atomic_add64(FAU_OUTSTANDING, 1);
        }

        #ifdef SWAP_MAC_ADDR
        int is_ip = !work->word2.s.not_IP;
        #endif

        /* Build a PKO pointer to this packet */
        if (work->word2.s.bufs == 0)
        {
            /* Packet is entirely in the WQE. Give the WQE to PKO and have it
                free it */
            pko_command.s.total_bytes = cvmx_wqe_get_len(work);
            pko_command.s.segs = 1;
            packet_ptr.u64 = 0;
            if (use_ipd_no_wptr)
            {
                packet_ptr.s.pool = CVMX_FPA_PACKET_POOL;
                packet_ptr.s.size = CVMX_FPA_PACKET_POOL_SIZE;
            }
            else
            {
                packet_ptr.s.pool = CVMX_FPA_WQE_POOL;
                packet_ptr.s.size = CVMX_FPA_WQE_POOL_SIZE;
            }
            packet_ptr.s.addr = cvmx_ptr_to_phys(work->packet_data);
            if (cvmx_likely(!work->word2.s.not_IP))
            {
                /* The beginning of the packet moves for IP packets */
                if (work->word2.s.is_v6)
                    packet_ptr.s.addr += 2;
                else
                    packet_ptr.s.addr += 6;
            }
        }
        else
        {
            pko_command.s.total_bytes = cvmx_wqe_get_len(work);
            pko_command.s.segs = work->word2.s.bufs;
            packet_ptr = work->packet_ptr;
            if (!use_ipd_no_wptr)
                cvmx_fpa_free(work, CVMX_FPA_WQE_POOL, 0);
        }

        /* For SRIO interface, build the header and remove SRIO RX word 0 */
        if (octeon_has_feature(OCTEON_FEATURE_SRIO) && port >= 40 && port < 44)
        {
            if (cvmx_srio_omsg_desc(port, &packet_ptr, NULL) >= 0)
                pko_command.s.total_bytes -= 8;
        }

        #ifdef SWAP_MAC_ADDR
        if (is_ip)
            swap_mac_addr((uint64_t)cvmx_phys_to_ptr((uint64_t)packet_ptr.s.addr));
        #endif

        #ifdef PEGASUS_PROC
        pegasus_packet_proc((uint64_t)cvmx_phys_to_ptr((uint64_t)packet_ptr.s.addr));
        #endif

        /*
         * Send the packet and wait for the tag switch to complete before
         * accessing the output queue. This ensures the locking required
         * for the queue.
         *
         */
#ifdef ENABLE_LOCKLESS_PKO
        ret = cvmx_pko_send_packet_finish(port, queue, pko_command,
	    packet_ptr, CVMX_PKO_LOCK_NONE);
#else
	if (octeon_has_feature(OCTEON_FEATURE_PKND))
	    ret = cvmx_pko_send_packet_finish_pkoid(pko_port, queue,
	        pko_command, packet_ptr, CVMX_PKO_LOCK_ATOMIC_TAG);
	else
	    ret = cvmx_pko_send_packet_finish(port, queue, pko_command,
	        packet_ptr, CVMX_PKO_LOCK_ATOMIC_TAG);
#endif
        if (ret)
        {
            printf("Failed to send packet using cvmx_pko_send_packet_finish\n");
            cvmx_fau_atomic_add64(FAU_ERRORS, 1);
        }
        holds_atomic_tag = 1;
    }
}



/**
 * Determine if a number is approximately equal to a match
 * value. Checks if the supplied value is within 5% of the
 * expected value.
 *
 * @param value    Value to check
 * @param expected Value needs to be within 5% of this value.
 * @return Non zero if the value is out of range.
 */
static int cycle_out_of_range(float value, float expected)
{
    uint64_t range = expected / 5; /* 5% */
    if (range<1)
        range = 1;

    /* The expected time check is disabled for Linux right now. Since the
        Linux kernel is configured for a 6 Mhz clock, there are a couple
        of context switch during this test. On the real chip the clock will
        be set the real value (600 Mhz) alleviating this problem. */
#ifndef __linux__
    return ((value < expected - range) || (value > expected + range));
#else
    return 0;
#endif
}


/**
 * Perform application specific shutdown
 *
 * @param num_processors
 *               The number of processors available.
 */
static void application_shutdown(int num_processors)
{
    uint64_t run_cycles = stop_cycle - start_cycle;
    float cycles_packet;

    /* The following speed checks assume you are using the original test data
        and executing with debug turned off. */
    const float * expected_cycles;
    const float cn68xx_cycles[32] = {319.0, 166.0, 118.0, 91.0, 78.0, 68.0, 62.0, 57.0, 55.0, 55.0, 55.0, 55.0, 55.0, 55.0, 56.0, 56.0, 56.0, 56.0, 56.0, 57.0, 57.0, 58.0, 57.0, 58.0, 58.0, 59.0, 59.0, 59.0, 59.0, 60.0, 61.0, 61.0};
    const float cn6xxx_cycles[10] = {328.0, 162.0, 116.0, 82.0, 71.0, 61.0, 53.0, 48.0, 44.0, 41.0};
    const float cn50xx_cycles[2] = {282.0, 156.0};
    const float cn3xxx_cycles[16] = {244.0, 123.0, 90.0, 63.0, 55.0, 47.0, 42.0, 39.0, 38.0, 38.0, 38.0, 38.0, 38.0, 38.0, 38.0, 38.0};
    const float cn3020_cycles[2] = {272.0, 150.0};
    const float cn3010_cycles[1] = {272.0};
    const float cn3005_cycles[1] = {315.0};

    if (OCTEON_IS_MODEL(OCTEON_CN3005))
        expected_cycles = cn3005_cycles;
    else if (OCTEON_IS_MODEL(OCTEON_CN3020))
        expected_cycles = cn3020_cycles;
    else if (OCTEON_IS_MODEL(OCTEON_CN30XX))
        expected_cycles = cn3010_cycles;
    else if (OCTEON_IS_MODEL(OCTEON_CN50XX))
        expected_cycles = cn50xx_cycles;
    else if (OCTEON_IS_MODEL(OCTEON_CN68XX))
        expected_cycles = cn68xx_cycles;
    else if (OCTEON_IS_MODEL(OCTEON_CN6XXX) || OCTEON_IS_MODEL(OCTEON_CNF7XXX))
        expected_cycles = cn6xxx_cycles;
    else
        expected_cycles = cn3xxx_cycles;

    /* Display a rough calculation for the cycles/packet. If you need
        accurate results, run lots of packets. */
    uint64_t count = cvmx_fau_fetch_and_add64(FAU_PACKETS, 0);
    cycles_packet = run_cycles / (float)count;
    printf("Total %lld packets in %lld cycles (%2.2f cycles/packet)[expected %2.2f cycles/packet]\n",
           (unsigned long long)count, (unsigned long long)run_cycles, cycles_packet, expected_cycles[num_processors-1]);

    if (cycle_out_of_range(cycles_packet, expected_cycles[num_processors-1]))
        printf("Cycles-per-packet is larger than the expected!\n");

    /* Display the results if a failure was detected. */
    if (cvmx_fau_fetch_and_add64(FAU_ERRORS, 0))
        printf("Errors detected. TEST FAILED\n");

    /* Wait for PKO to complete */
    printf("Waiting for packet output to finish\n");
    while (cvmx_fau_fetch_and_add64(FAU_OUTSTANDING, 0) != 0)
    {
        /* Spinning again */
    }

    /* Delay so the last few packets make it out. The fetch and add
        is a little ahead of the hardware */
    cvmx_wait(1000000);
}

/**
 * Main entry point
 *
 * @return exit code
 */
int main(int argc, char *argv[])
{
    cvmx_sysinfo_t *sysinfo;
    unsigned int coremask_passthrough;
    int result = 0;

#ifndef ENABLE_PASSTHROUGH_HOTPLUG
#define CORE_MASK_BARRIER_SYNC\
        cvmx_coremask_barrier_sync(coremask_passthrough)
#define IS_INIT_CORE\
    (cvmx_coremask_first_core(coremask_passthrough))
#else
#define CORE_MASK_BARRIER_SYNC if (!is_core_being_hot_plugged())\
        cvmx_coremask_barrier_sync(coremask_passthrough)
#define IS_INIT_CORE\
    (cvmx_coremask_first_core(coremask_passthrough) && !is_core_being_hot_plugged())
#endif

    cvmx_user_app_init();

#ifdef ENABLE_PASSTHROUGH_HOTPLUG
    if (init_hotplug())
        return -1;
#endif

    /* compute coremask_passthrough on all cores for the first barrier sync below */
    sysinfo = cvmx_sysinfo_get();
    coremask_passthrough = sysinfo->core_mask;

    if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
    {
        if (OCTEON_IS_MODEL(OCTEON_CN3005) || OCTEON_IS_MODEL(OCTEON_CNF71XX))
            packet_termination_num = 3032;
        else if (OCTEON_IS_MODEL(OCTEON_CN31XX) || OCTEON_IS_MODEL(OCTEON_CN3010) || OCTEON_IS_MODEL(OCTEON_CN50XX))
            packet_termination_num = 4548;
        else
//#define SINGLE_PORT_SIM
#ifdef SINGLE_PORT_SIM
            packet_termination_num = 1516;
#else
            packet_termination_num = 6064;
#endif
    }
    else
       packet_termination_num = 1000;

    /*
     * elect a core to perform boot initializations, as only one core needs to
     * perform this function.
     *
     */

    if (IS_INIT_CORE) {
        printf("Version: %s\n", cvmx_helper_get_version());

        if (octeon_has_feature(OCTEON_FEATURE_SRIO))
        {
            if (cvmx_helper_interface_get_mode(4) == CVMX_HELPER_INTERFACE_MODE_SRIO)
                cvmx_srio_initialize(0, 0);
            if (cvmx_helper_interface_get_mode(5) == CVMX_HELPER_INTERFACE_MODE_SRIO)
                cvmx_srio_initialize(1, 0);
        }

        /* 64 is the minimum number of buffers that are allocated to receive
           packets, but the real hardware, allocate above this minimal number. */
        if ((result = application_init_simple_exec(packet_termination_num+80)) != 0) {
            printf("Simple Executive initialization failed.\n");
            printf("TEST FAILED\n");
            return result;
        }
    }

    CORE_MASK_BARRIER_SYNC;


#ifdef ENABLE_LOCKLESS_PKO
    /* First core do some runtime sanity check.
       Make sure there is enough queues for each core online */
    if (IS_INIT_CORE) {
        int cores_online = cvmx_pop(coremask_passthrough);

            if ((cores_online  > CVMX_PKO_QUEUES_PER_PORT_INTERFACE0)
            || (cores_online > CVMX_PKO_QUEUES_PER_PORT_INTERFACE1))  {
            printf ("Lockless PKO operation requires each running\n");
            printf ("core to use a dedicated PKO queue for each port\n");
                   printf ("%d cores are online\n", cores_online);
            printf ("Interface 0 has %d queues per port \n",
                                       CVMX_PKO_QUEUES_PER_PORT_INTERFACE0);
            printf ("Interface 1 has %d queues per port \n",
                                       CVMX_PKO_QUEUES_PER_PORT_INTERFACE1);
            printf ("Failed to enable Lockless PKO   \n");
            return -1;
        }
        printf("Enable Lockless PKO\n");
    }
    CORE_MASK_BARRIER_SYNC;
#endif

    cvmx_helper_initialize_packet_io_local();

    /* Remember when we started the test.  For accurate numbers it needs to be as
       close as possible to the running of the application main loop. */
    if (IS_INIT_CORE) {
        if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM)
        {
            printf("Waiting to give packet input (~1Gbps) time to read the packets...\n");
            if (OCTEON_IS_MODEL(OCTEON_CN68XX))
            {
                cvmx_sso_iq_com_cnt_t sso_iq_com_cnt;
                do
                {
                    sso_iq_com_cnt.u64 = cvmx_read_csr(CVMX_SSO_IQ_COM_CNT);
#ifdef DUMP_STATS
                    printf("sso_iq_com_cnt.u64 = %lu\n", sso_iq_com_cnt.u64);
#endif
                } while (sso_iq_com_cnt.s.iq_cnt < packet_termination_num);
            }
            else
            {
                cvmx_pow_iq_com_cnt_t pow_iq_com_cnt;
                do
                {
                    pow_iq_com_cnt.u64 = cvmx_read_csr(CVMX_POW_IQ_COM_CNT);
                } while (pow_iq_com_cnt.s.iq_cnt < packet_termination_num);
            }
            printf("Done waiting\n");
        }

        start_cycle = cvmx_get_cycle();
    }

    CORE_MASK_BARRIER_SYNC;
    application_main_loop();

#ifdef ENABLE_PASSTHROUGH_HOTPLUG
    if (core_unplug_requested)
        cvmx_app_hotplug_core_shutdown();

    coremask_passthrough = sysinfo->core_mask;
    printf("Shutdown coremask=%08x\n", coremask_passthrough);
#endif

    cvmx_coremask_barrier_sync(coremask_passthrough);


    /* Remember when we stopped the test. This could have been done in the
       application_shutdown, but for accurate numbers it needs to be as close as
       possible to the running of the application main loop. */
    if (cvmx_coremask_first_core(coremask_passthrough)) {

        stop_cycle = cvmx_get_cycle();
    }
    cvmx_coremask_barrier_sync(coremask_passthrough);

#if CVMX_PKO_USE_FAU_FOR_OUTPUT_QUEUES
    /* Free the prefetched output queue buffer if allocated */
    {
        void * buf_ptr = cvmx_phys_to_ptr(cvmx_scratch_read64(CVMX_SCR_OQ_BUF_PRE_ALLOC));
        if (buf_ptr)
            cvmx_fpa_free(buf_ptr, CVMX_FPA_OUTPUT_BUFFER_POOL, 0);
    }
#endif

    /* use core 0 to perform application shutdown as well. */
    if (cvmx_sysinfo_get()->board_type == CVMX_BOARD_TYPE_SIM && cvmx_coremask_first_core(coremask_passthrough))
    {

        int num_processors;
        CVMX_POP(num_processors, coremask_passthrough);
        application_shutdown(num_processors);

        if ((result = application_shutdown_simple_exec()) != 0) {
            printf("Simple Executive shutdown failed.\n");
            printf("TEST FAILED\n");
        }
    }

    cvmx_coremask_barrier_sync(coremask_passthrough);
#ifdef ENABLE_PASSTHROUGH_HOTPLUG
    if (app_shutdown_requested)
        cvmx_app_hotplug_core_shutdown();
#endif

    return result;
}
