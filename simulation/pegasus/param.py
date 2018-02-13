"""
param.py: Parameters and configuration options for the simulator.
"""

import random

NODE_MSG_QUEUE_LENGTH = 1024

MED_PROPG_DELAY = 50
PROPG_DELAY_SD = 2.5
MIN_PROPG_DELAY = round(MED_PROPG_DELAY - 2 * PROPG_DELAY_SD)
MAX_PROPG_DELAY = round(MED_PROPG_DELAY + 2 * PROPG_DELAY_SD)

MED_PKT_PROC_LTC = 5
PKT_PROC_LTC_SD = 0.5
MIN_PKT_PROC_LTC = round(MED_PKT_PROC_LTC - 2 * PKT_PROC_LTC_SD)
MAX_PKT_PROC_LTC = round(MED_PKT_PROC_LTC + 2 * PKT_PROC_LTC_SD)

def propg_delay():
    delay = round(MED_PROPG_DELAY + random.gauss(0, PROPG_DELAY_SD))
    if delay < MIN_PROPG_DELAY:
        return MIN_PROPG_DELAY
    if delay > MAX_PROPG_DELAY:
        return MAX_PROPG_DELAY
    return delay

def pkt_proc_ltc():
    ltc = round(MED_PKT_PROC_LTC + random.gauss(0, PKT_PROC_LTC_SD))
    if ltc < MIN_PKT_PROC_LTC:
        return MIN_PKT_PROC_LTC
    if ltc > MAX_PKT_PROC_LTC:
        return MAX_PKT_PROC_LTC
    return ltc
