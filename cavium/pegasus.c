#include <string.h>
#include "pegasus.h"

/*
 * Hard-coded addresses
 */
#define MAX_NUM_NODES 16
static node_address_t node_addresses[MAX_NUM_NODES] = {
    [0] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12345 },
    [1] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12346 },
    [2] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12347 },
    [3] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12348 },
};

/*
 * Global variables
 */
static size_t num_nodes = 1;

/*
 * Static function declarations
 */
static void convert_endian(void *dst, const void *src, size_t n);
static int match_pegasus_packet(uint64_t buf);
static int decode_packet(uint64_t buf, request_t *request);
static uint64_t key_hash(const char* key);
static void forward_to_node(uint64_t buf, int node_id);
static uint64_t checksum(const void *buf, size_t n);

/*
 * Static function definitions
 */
static void convert_endian(void *dst, const void *src, size_t n)
{
  size_t i;
  uint8_t *dptr, *sptr;

  dptr = (uint8_t*)dst;
  sptr = (uint8_t*)src;

  for (i = 0; i < n; i++) {
    *(dptr + i) = *(sptr + n - i - 1);
  }
}

static int match_pegasus_packet(uint64_t buf)
{
    identifier_t identifier;
    convert_endian(&identifier, (const void *)(buf+APP_HEADER), sizeof(identifier_t));
    if (identifier == IDENTIFIER) {
        return 0;
    }
    return -1;
}

static int decode_packet(uint64_t buf, request_t *request)
{
    uint64_t ptr = buf;
    ptr += sizeof(identifier_t);
    type_t type = *(type_t *)ptr;
    ptr += sizeof(type_t);
    if (type != TYPE_REQUEST) {
        return -1;
    }
    ptr += sizeof(client_id_t) + sizeof(req_id_t);
    request->op_type = *(op_type_t *)ptr;
    ptr += sizeof(op_type_t) + sizeof(key_len_t);
    request->key = (const char*)ptr;
    return 0;
}

static uint64_t key_hash(const char* key)
{
    uint64_t hash = 5381;
    size_t i;
    for (i = 0; i < strlen(key); i++) {
        hash = ((hash << 5) + hash) + (uint64_t)key[i];
    }
    return hash;
}

static void forward_to_node(uint64_t buf, int node_id)
{
    // Modify MAC address
    size_t i;
    for (i = 0; i < 6; i++) {
        *(uint8_t *)(buf + ETH_DST + i) = node_addresses[node_id].mac_addr[i];
    }

    // Modify IP address
    *(uint32_t *)(buf + IP_DST) = node_addresses[node_id].ip_addr;
    *(uint16_t *)(buf + IP_CKSUM) = 0;
    *(uint16_t *)(buf + IP_CKSUM) = (uint16_t)checksum((void *)(buf + IP_HEADER), IP_SIZE);

    // Modify UDP address
    *(uint16_t *)(buf + UDP_DST) = node_addresses[node_id].port;
    *(uint16_t *)(buf + UDP_CKSUM) = 0;
}

static uint64_t checksum(const void *buf, size_t n)
{
    uint64_t sum;
    uint16_t *ptr = (uint16_t *)buf;
    size_t i;
    for (i = 0, sum = 0; i < (n / 2); i++) {
        sum += *ptr++;
    }
    sum = (sum >> 16) + (sum & 0xFFFF);
    sum += (sum >> 16);
    return ~sum;
}

/*
 * Public function definitions
 */
void pegasus_packet_proc(uint64_t buf)
{
    if (match_pegasus_packet(buf) == 0) {
        request_t request;
        if (decode_packet(buf+APP_HEADER, &request) == 0) {
            int node_id = key_hash(request.key) % num_nodes;
            forward_to_node(buf, node_id);
        }
    }
}
