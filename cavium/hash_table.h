#ifndef _HASH_TABLE_H_
#define _HASH_TABLE_H_

#include <stddef.h>
#include <assert.h>
#include "cvmx.h"
#include "cvmx-spinlock.h"

//#define USE_NIC_MEMORY

#define CACHE_ALIGN __attribute__((aligned(128)))
//#define MY_ASSERT(x) assert(x)
#define MY_ASSERT(x) do {} while (0)

typedef enum _ht_status {
    HT_FOUND,
    HT_NOT_FOUND,
    HT_INSERT_SUCCESS,
    HT_INSERT_FAILURE_TABLE_FULL,
    HT_INSERT_FAILURE_KEY_DUPLICATED,
    HT_DELETE_SUCCESS,
    HT_DELETE_FAILURE_NOT_FOUND,
} ht_status;

typedef struct _concurrent_ht_t {
    size_t hash_power;          /* 2 ^ hash_power buckets in total */
    size_t hash_total_items;    /* Total number of items in the hashtable */
    size_t kick_out;            /* kick out during one insert operation */
    void *buckets;              /* pointer to the array of buckets */
    void *keyver_array;         /* an array of version counters */
    void *cuckoo_path;          /* record the cuckoo path during displacent */
    cvmx_spinlock_t lock;       /* single writer lock */
} CACHE_ALIGN concurrent_ht_t;

/*
 * @brief Initialize the hash table
 *
 * @init_hash_power: the logarithm of the inital table size
 * @return: the handler to the hashtable on success, NULL on failure
 */
concurrent_ht_t* concur_hashtable_init(const size_t init_hashpower);

/*
 * @brief Clean up the hash table
 *
 * @ht: the handler of the hashtable
 */
void concur_hashtable_free(concurrent_ht_t *ht);

/*
 * @brief Insert the <key, val> pair into the hashtable
 *
 * @ht: the handler of the hashtable
 * @key: key to inserted
 * @val: value to inserted
 * @return: the hashtable status
 */
ht_status concur_hashtable_insert(concurrent_ht_t *ht,
                                  const char *key,
                                  const char *val);

/*
 * @brief Find the key from the hashtable and write into the val
 *
 * @ht: the handler of the hashtable
 * @key: the found key
 * @val: value to write
 * @return: the hashtable status
 */
ht_status concur_hashtable_find(concurrent_ht_t *ht,
                                const char *key,
                                char *val);

/*
 * @breif Delete the key from the hashtable
 *
 * @ht: the handler of the hashtable
 * @key: the deleted key
 * @return: the hashtable status
 */
ht_status concur_hashtable_delete(concurrent_ht_t *ht,
                                  const char *key);

/*
 * @brief Print the hashtable operation status
 *
 * @s: the HT status
 */
void concur_hashtable_print_status(ht_status s);

/*
 * @bried Report the hashtable information
 *
 * @ht: the handler of the hashtable
 */
void concur_hashtable_report(concurrent_ht_t *ht);

#endif /* _HASH_TABLE_H_ */
