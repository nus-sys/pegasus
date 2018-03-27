#include <string.h>
#include "nic_mem.h"
#include "hash_table.h"
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
    [4] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12349 },
    [5] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12350 },
    [6] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12351 },
    [7] = { .mac_addr = {0xE4, 0x1D, 0x2D, 0x2E, 0x35, 0x11},
        .ip_addr = 0x0A0A0107,
        .port = 12352 },
};
#define PORT_ZERO 12345

/*
 * Global variables
 */
static size_t num_nodes = 1;
static node_load_t node_loads[MAX_NUM_NODES];
static float load_constant = 1.1;
static concurrent_ht_t *key_node_map = NULL;
static concurrent_ht_t *key_rates = NULL;
static lb_type_t lb_type = LB_STATIC;
static uint64_t init_cycle = 0;

/*
 * Static function declarations
 */
static int init();
static void convert_endian(void *dst, const void *src, size_t n);
static packet_type_t match_pegasus_packet(uint64_t buf);
static int decode_kv_packet(uint64_t buf, kv_packet_t *kv_packet);
static uint64_t key_hash(const char* key);
static void forward_to_node(uint64_t buf, const dest_node_t *dest_node);
static uint64_t checksum(const void *buf, size_t n);
static int decode_controller_packet(uint64_t buf, reset_t *reset);
static void process_kv_packet(uint64_t buf, const kv_packet_t *kv_packet);
static void process_controller_packet(uint64_t buf, const reset_t *reset);
static int port_to_node_id(uint16_t port);
static dest_node_t key_to_dest_node(const char *key);
static void update_load_request(const dest_node_t *dest_node, const char *key);
static void update_load_reply(int node_id, const char *key);
static uint32_t calc_key_rate(const key_rate_t *key_rate);
static uint32_t calc_time_from_cycles(uint64_t cycles);

/*
 * Static function definitions
 */
static int init()
{
    printf("Pegasus initializing... Number of nodes: %lu LB type %u\n",
            num_nodes, lb_type);
    size_t i;
    for (i = 0; i < num_nodes; i++) {
        node_loads[i].pload = 0;
        node_loads[i].iload = 0;
    }
    key_node_map = concur_hashtable_init(0);
    if (key_node_map == NULL) {
        printf("Failed to initialize key node map\n");
        return -1;
    }
    key_rates = concur_hashtable_init(0);
    if (key_rates == NULL) {
        printf("Failed to initialize key rates\n");
        return -1;
    }
    init_cycle = 0;
    return 0;
}

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

static packet_type_t match_pegasus_packet(uint64_t buf)
{
    identifier_t identifier;
    convert_endian(&identifier, (const void *)(buf+APP_HEADER), sizeof(identifier_t));
    if (identifier == KV_ID) {
        return KV;
    } else if (identifier == CONTROLLER_ID) {
        return CONTROLLER;
    } else {
        return UNKNOWN;
    }
}

static int decode_kv_packet(uint64_t buf, kv_packet_t *kv_packet)
{
    uint64_t ptr = buf;
    ptr += sizeof(identifier_t);
    type_t type = *(type_t *)ptr;
    ptr += sizeof(type_t);
    if (type != TYPE_REQUEST && type != TYPE_REPLY) {
        return -1;
    }
    kv_packet->type = type;
    if (kv_packet->type == TYPE_REQUEST) {
        ptr += sizeof(client_id_t) + sizeof(req_id_t) + sizeof(node_id_t);
        kv_packet->op_type = *(op_type_t *)ptr;
        ptr += sizeof(op_type_t) + sizeof(key_len_t);
        kv_packet->key = (const char*)ptr;
    }
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

static void forward_to_node(uint64_t buf, const dest_node_t *dest_node)
{
    // Modify MAC address
    size_t i;
    for (i = 0; i < 6; i++) {
        *(uint8_t *)(buf + ETH_DST + i) = node_addresses[dest_node->forward_node_id].mac_addr[i];
    }

    // Modify IP address
    *(uint32_t *)(buf + IP_DST) = node_addresses[dest_node->forward_node_id].ip_addr;
    *(uint16_t *)(buf + IP_CKSUM) = 0;
    *(uint16_t *)(buf + IP_CKSUM) = (uint16_t)checksum((void *)(buf + IP_HEADER), IP_SIZE);

    // Modify UDP address
    *(uint16_t *)(buf + UDP_DST) = node_addresses[dest_node->forward_node_id].port;
    *(uint16_t *)(buf + UDP_CKSUM) = 0;

    // Set migration node id
    if (dest_node->migration_node_id >= 0) {
        node_id_t node_id = dest_node->migration_node_id + 1;
        convert_endian((void *)(buf+APP_HEADER+sizeof(identifier_t)+sizeof(type_t)+sizeof(client_id_t)+sizeof(req_id_t)), &node_id, sizeof(node_id_t));
    }
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

static int decode_controller_packet(uint64_t buf, reset_t *reset)
{
    uint64_t ptr = buf;
    ptr += sizeof(identifier_t);
    type_t type = *(type_t *)ptr;
    ptr += sizeof(type_t);
    if (type != TYPE_RESET) {
        return -1;
    }
    convert_endian(&reset->num_nodes, (const void *)ptr, sizeof(num_nodes_t));
    ptr += sizeof(num_nodes_t);
    reset->lb_type = *(lb_type_t *)ptr;
    return 0;
}

static void process_kv_packet(uint64_t buf, const kv_packet_t *kv_packet)
{
    if (kv_packet->type == TYPE_REQUEST) {
        dest_node_t dest_node = key_to_dest_node(kv_packet->key);
        update_load_request(&dest_node, kv_packet->key);
        forward_to_node(buf, &dest_node);
    } else if (kv_packet->type == TYPE_REPLY) {
        int node_id = port_to_node_id(*(uint16_t *)(buf+UDP_SRC));
        update_load_reply(node_id, kv_packet->key);
    }
}

static void process_controller_packet(uint64_t buf, const reset_t *reset)
{
    num_nodes = reset->num_nodes;
    lb_type = reset->lb_type;
    concur_hashtable_free(key_node_map);
    concur_hashtable_free(key_rates);
    init();
}

static int port_to_node_id(uint16_t port)
{
    return port - PORT_ZERO;
}

static dest_node_t key_to_dest_node(const char *key)
{
    uint64_t keyhash = key_hash(key);
    dest_node_t dest_node = {-1, -1};

    if (lb_type == LB_STATIC) {
        dest_node.forward_node_id = (int)(keyhash % num_nodes);
    } else {
        size_t i;
        uint32_t total_iload = 0;
        uint32_t total_pload = 0;
        for (i = 0; i < num_nodes; i++) {
            total_iload += node_loads[i].iload;
            total_pload += node_loads[i].pload;
        }
        uint32_t avg_iload = total_iload / num_nodes;
        uint32_t avg_pload = total_pload / num_nodes;

        // Get mapped node
        int curr_node_id, *node_id_val;
        size_t val_size;
        ht_status ret;
        ret = concur_hashtable_find(key_node_map, key, (void **)&node_id_val, &val_size);
        curr_node_id = ret == HT_FOUND ? *node_id_val : (int)(keyhash % num_nodes);
        dest_node.forward_node_id = curr_node_id;

        // Check if node is overloaded
        if (node_loads[curr_node_id].iload > load_constant * avg_iload &&
                node_loads[curr_node_id].pload > load_constant * avg_pload) {
            int node_found = 0;
            int next_node_id = (curr_node_id + 1) % num_nodes;
            // Find a node that is under-loaded
            for (i = 0; i < num_nodes - 1; i++) {
                if (node_loads[next_node_id].iload <= load_constant * avg_iload &&
                        node_loads[next_node_id].pload <= load_constant * avg_pload) {
                    node_found = 1;
                    break;
                }
                next_node_id = (next_node_id + 1) % num_nodes;
            }
            if (node_found) {
                dest_node.migration_node_id = next_node_id;

                // Update key -> node mapping
                if (ret == HT_FOUND) {
                    *node_id_val = next_node_id;
                } else {
                    ret = concur_hashtable_insert(key_node_map, key, &next_node_id, sizeof(int));
                    if (ret != HT_INSERT_SUCCESS) {
                        printf("Failed to insert into key_node_map, error %u\n", ret);
                    }
                }

                // Update pload
                key_rate_t *key_rate;
                size_t key_rate_len;
                ret = concur_hashtable_find(key_rates, key, (void **)&key_rate, &key_rate_len);
                if (ret == HT_FOUND) {
                    node_loads[curr_node_id].pload -= calc_key_rate(key_rate);
                    node_loads[next_node_id].pload += calc_key_rate(key_rate);
                }
            }
        }
    }
    return dest_node;
}

static void update_load_request(const dest_node_t *dest_node, const char *key)
{
    node_loads[dest_node->forward_node_id].iload++;
    if (init_cycle == 0) {
        init_cycle = cvmx_clock_get_count(CVMX_CLOCK_CORE);
    }

    ht_status ret;
    uint32_t old_rate, new_rate;
    key_rate_t *key_rate;
    size_t key_rate_len;
    ret = concur_hashtable_find(key_rates, key, (void **)&key_rate, &key_rate_len);
    uint64_t curr_cycle = cvmx_clock_get_count(CVMX_CLOCK_CORE);
    uint32_t time = calc_time_from_cycles(curr_cycle - init_cycle);
    if (ret == HT_FOUND) {
        old_rate = calc_key_rate(key_rate);
        key_rate->count++;
        key_rate->time = time;
        new_rate = calc_key_rate(key_rate);
    } else {
        old_rate = new_rate = 0; // do not calculate rate for the first access
        key_rate_t new_key_rate;
        new_key_rate.count = 1;
        new_key_rate.time = time;
        ret = concur_hashtable_insert(key_rates, key, &new_key_rate, sizeof(key_rate_t));
        if (ret != HT_INSERT_SUCCESS) {
            printf("Failed to insert into key_rates, error %u\n", ret);
        }
    }

    // Update node's pload
    int node_id = dest_node->migration_node_id >= 0 ? dest_node->migration_node_id : dest_node->forward_node_id;
    node_loads[node_id].pload += (new_rate - old_rate);
}

static void update_load_reply(int node_id, const char *key)
{
    if (node_loads[node_id].iload > 0) {
        node_loads[node_id].iload--;
    }
}

static uint32_t calc_key_rate(const key_rate_t *key_rate)
{
    if (key_rate->count <= 1) {
        return 0;
    }
    return ((uint64_t)key_rate->count * 1000000) / key_rate->time;
}

static uint32_t calc_time_from_cycles(uint64_t cycles)
{
    return cycles / (cvmx_clock_get_rate(CVMX_CLOCK_CORE) / 1000000);
}

/*
 * Public function definitions
 */
int pegasus_init()
{
#ifdef USE_NIC_MEMORY
    nic_local_shared_mm_init();
#endif
    return init();
}

void pegasus_packet_proc(uint64_t buf)
{
    switch (match_pegasus_packet(buf)) {
        case KV: {
            kv_packet_t kv_packet;
            if (decode_kv_packet(buf+APP_HEADER, &kv_packet) == 0) {
                process_kv_packet(buf, &kv_packet);
            }
            break;
        }
        case CONTROLLER: {
            reset_t reset;
            if (decode_controller_packet(buf+APP_HEADER, &reset) == 0) {
                process_controller_packet(buf, &reset);
            }
            break;
        }
        default:
            return;
    }
}
