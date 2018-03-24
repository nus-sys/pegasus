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
 * KV message header fields
 */
typedef uint32_t identifier_t;
typedef uint8_t type_t;
typedef uint32_t client_id_t;
typedef uint32_t req_id_t;
typedef uint32_t node_id_t;
typedef uint8_t op_type_t;
typedef uint16_t key_len_t;
typedef uint8_t result_t;
typedef uint16_t value_len_t;

static const identifier_t KV_ID = 0xDEADBEEF;
static const type_t TYPE_REQUEST = 1;
static const type_t TYPE_REPLY = 2;
static const op_type_t GET = 0;
static const op_type_t PUT = 1;
static const op_type_t DEL = 2;

/*
 * Controller message header fields
 */
typedef uint32_t num_nodes_t;

static const identifier_t CONTROLLER_ID = 0xDEADDEAD;
static const type_t TYPE_RESET = 1;

/*
 * Data structures
 */
typedef enum {
    KV,
    CONTROLLER,
    UNKNOWN
} packet_type_t;

typedef struct {
    uint8_t mac_addr[6];
    uint32_t ip_addr;
    uint16_t port;
} node_address_t;

typedef struct {
    type_t type;
    op_type_t op_type;
    const char* key;
} kv_packet_t;

typedef struct {
    uint32_t num_nodes;
} reset_t;

typedef struct {
    uint64_t iload;
} node_load_t;

typedef struct {
    int forward_node_id;
    int migration_node_id;
} dest_node_t;

/*
 * Pegasus packet processing functions
 */
void pegasus_packet_proc(uint64_t buf);

#endif /* __PEGASUS_H__ */
