#ifndef __PEGASUS_H__
#define __PEGASUS_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define PEGASUS_PROC

/*
 * Packet macros
 */
#define ETH_DST     0
#define ETH_SRC     6
#define IP_SIZE     20
#define IP_HEADER   14
#define IP_CKSUM    24
#define IP_SRC      26
#define IP_DST      30
#define UDP_SRC     34
#define UDP_DST     36
#define UDP_CKSUM   40
#define APP_HEADER  42

/*
 * Pegasus header fields
 */
typedef uint32_t identifier_t;
typedef uint8_t type_t;
typedef uint32_t client_id_t;
typedef uint32_t req_id_t;
typedef uint8_t op_type_t;
typedef uint16_t key_len_t;
typedef uint8_t result_t;
typedef uint16_t value_len_t;

static const identifier_t IDENTIFIER = 0xDEADBEEF;
static const type_t TYPE_REQUEST = 1;
static const type_t TYPE_REPLY = 2;
static const op_type_t GET = 0;
static const op_type_t PUT = 1;
static const op_type_t DEL = 2;

/*
 * Data structures
 */
typedef struct {
    uint8_t mac_addr[6];
    uint32_t ip_addr;
    uint16_t port;
} node_address_t;

typedef struct {
    op_type_t op_type;
    const char* key;
} request_t;

/*
 * Pegasus packet processing functions
 */
void pegasus_packet_proc(uint64_t buf);

#endif /* __PEGASUS_H__ */
