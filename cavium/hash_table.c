#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#include "hash_table.h"
#include "city_hash.h"

#define HASHPOWER_DEFAULT 16                    /* default hash table size */
#define BUCKET_SIZE       4                     /* 4-way set-associate     */
#define KEY_SIZE          128                   /* Maximum size of the key */
#define VAL_SIZE          128                   /* Maximum size of the value */
#define KEY_VER_SIZE      ((uint32_t) 1 << 13)  /* key version size */
#define KEY_VER_MASK      (KEY_VER_SIZE - 1)    /* key version mask */
#define HASH_SIZE(n)      ((uint32_t) 1 << n)   /* hash size */
#define HASH_MASK(n)      (HASH_SIZE(n) - 1)    /* hash mask */
#define NUM_CUCKOO_PATH   2                     /* number of CUCKOO path */
#define MAX_CUCKOO_COUNT  500                   /* maximum CUCKOO count */

/* key type in the hashtable */
typedef struct _key_t {
    size_t key_len;
    char key[KEY_SIZE];
} __attribute__((packed)) ht_key_t;

/* value type in the hashtable */
typedef struct _val_t {
    size_t val_len;
    char val[VAL_SIZE];
} __attribute__((packed)) ht_val_t;

/* <key, value> pair in the hashtable */
typedef struct _kv_t {
    ht_key_t key;
    ht_val_t val;
} __attribute__((packed)) ht_kv_t;

/* bucket type in the hashtable */
typedef struct _bucket_t {
    int32_t tags[BUCKET_SIZE];
    ht_kv_t* kv_data[BUCKET_SIZE];
} CACHE_ALIGN ht_bucket_t;

/* cuckoo_record type in the hashtable */
typedef struct _cuckoo_record {
    size_t buckets[NUM_CUCKOO_PATH];
    size_t slots[NUM_CUCKOO_PATH];
    ht_key_t keys[NUM_CUCKOO_PATH];
} __attribute__((packed)) cuckoo_record;

/* Hash of the key */
static inline uint32_t
get_key_hash(const char *key, size_t len)
{
    return CityHash32(key, len);
}

/* Hash of the first bucket */
static inline size_t
get_first_index (concurrent_ht_t *ht,
                 const uint32_t hv)
{
    return (hv & HASH_MASK(ht->hash_power));
}

/* Hash of the second bucket */
static inline size_t
get_second_index (concurrent_ht_t *ht,
                  const uint32_t hv,
                  const size_t index)
{
    // magic number (i.e. 0x5bd1e995) is the hash constant from MurmurHash2
    uint32_t tag = hv >> 24;
    return (index ^ (tag * 0x5bd1e995)) & HASH_MASK(ht->hash_power);
}

static inline void
mylock_init (concurrent_ht_t *ht)
{
    cvmx_spinlock_init(&ht->lock);
}

static inline void
mylock_destroy (concurrent_ht_t *ht)
{
}

static inline void
mylock_lock (concurrent_ht_t *ht)
{
    cvmx_spinlock_lock(&ht->lock);
}

static inline void
mylock_unlock (concurrent_ht_t *ht)
{
    cvmx_spinlock_unlock(&ht->lock);
}

static inline uint32_t
keylock_index (const uint32_t hv)
{
    return (hv & KEY_VER_MASK);
}

//#ifdef POSIX

#define KVC_START_READ(ht, index) \
  __sync_fetch_and_add(&((uint32_t *)ht->keyver_array)[index & KEY_VER_MASK], 0)

#define KVC_END_READ(ht, index, ret) \
    do { \
        asm volatile ("" ::: "memory"); \
        ret = ((uint32_t *)ht->keyver_array)[index & KEY_VER_MASK]; \
    } while (0)

#define KVC_START_INCR(ht, index) \
    do { \
        ((uint32_t *)ht->keyver_array)[index & KEY_VER_MASK] += 1; \
        asm volatile ("" ::: "memory"); \
    } while (0)

#define KVC_END_INCR(ht, index) \
 __sync_fetch_and_add(&((uint32_t *)ht->keyver_array)[index & KEY_VER_MASK], 1)

//#else
//#endif

#define TABLE_KV(ht, i, j)  ((ht_bucket_t *)ht->buckets)[i].kv_data[j]
#define TABLE_TAG(ht, i, j) ((ht_bucket_t *)ht->buckets)[i].tags[j]
#define TABLE_KEY(ht, i, j) ((ht_bucket_t *)ht->buckets)[i].kv_data[j]->key
#define TABLE_VAL(ht, i, j) ((ht_bucket_t *)ht->buckets)[i].kv_data[j]->val

static void
ht_key_copy(ht_key_t *des,
            ht_key_t *src)
{
    des->key_len = src->key_len;
    memcpy(des->key, src->key, src->key_len + 1);
}

static void
ht_val_copy(ht_val_t *des,
            ht_val_t *src)
{
    des->val_len = src->val_len;
    memcpy(des->val, src->val, src->val_len + 1);
}

static bool
ht_key_equal(ht_key_t key1,
             ht_key_t key2)
{
    if (key1.key_len != key2.key_len)
        return false;

    if (memcmp(key1.key, key2.key, key1.key_len))
        return false;

    return true;
}

static bool
key_is_equal(const char *key,
             ht_key_t ht_key)
{
    size_t key_len = strlen(key);

    if (key_len != ht_key.key_len)
        return false;

    if (memcmp(key, ht_key.key, key_len))
        return false;

    return true;
}

static bool
read_from_bucket(concurrent_ht_t *ht,
                 uint32_t tag,
                 size_t index,
                 const char *key,
                 char *val)
{
    size_t i;

    for (i = 0; i < BUCKET_SIZE; i++) {
        if ((int)tag == TABLE_TAG(ht, index, i)) {

            if (key_is_equal(key, TABLE_KEY(ht, index, i))) {
                memcpy(val, TABLE_VAL(ht, index, i).val,
                       TABLE_VAL(ht, index, i).val_len);
                val[TABLE_VAL(ht, index, i).val_len] = '\0';

                return true;
            }
        }
    }

    return false;
}

static inline bool
slot_is_empty(concurrent_ht_t *ht,
              size_t index,
              size_t sub_index)
{
    return ((TABLE_KEY(ht, index, sub_index).key_len == 0) ? true: false);
}


static bool
add_into_bucket(concurrent_ht_t *ht,
                const char *key,
                const char *val,
                uint32_t hv,
                size_t index,
                uint32_t keylock)
{
    size_t i, key_len, val_len;

    for (i = 0; i < BUCKET_SIZE; i++) {
        if (slot_is_empty(ht, index, i)) {
            KVC_START_INCR(ht, keylock);

            key_len = strlen(key);
            val_len = strlen(val);

            MY_ASSERT( ((key_len >= 0) && (key_len <= KEY_SIZE)) );
            MY_ASSERT( ((val_len >= 0) && (val_len <= VAL_SIZE)) );

            TABLE_TAG(ht, index, i) = hv >> 24;
            TABLE_KEY(ht, index, i).key_len = key_len;
            memcpy(TABLE_KEY(ht, index, i).key, key, key_len);
            TABLE_VAL(ht, index, i).val_len = val_len;
            memcpy(TABLE_VAL(ht, index, i).val, val, val_len);

            ht->hash_total_items++;

            KVC_END_INCR(ht, keylock);

            return true;
        }
    }

    return false;
}

static bool
delete_from_bucket(concurrent_ht_t *ht,
                   const char *key,
                   size_t index,
                   uint32_t keylock)
{
    size_t i;

    for (i = 0; i < BUCKET_SIZE; i++) {
        if (key_is_equal(key, TABLE_KEY(ht, index, i))) {
            KVC_START_INCR(ht, keylock);

            TABLE_TAG(ht, index, i) = 0;
            TABLE_VAL(ht, index, i).val_len = 0;

            ht->hash_total_items--;

            KVC_END_INCR(ht, keylock);
            return true;
        }
    }

    return false;
}

static ht_status
cuckoo_find(concurrent_ht_t *ht,
            const char *key,
            char *val,
            uint32_t hv,
            size_t i1,
            size_t i2,
            uint32_t keylock)
{
    bool ret;
    uint32_t vc_start, vc_end, tag;

    tag = hv >> 24;

try_read:
    vc_start = KVC_START_READ(ht, keylock);

    ret = read_from_bucket(ht, tag, i1, key, val);
    if (!ret) {
        ret = read_from_bucket(ht, tag, i2, key, val);
    }
    KVC_END_READ(ht, keylock, vc_end);

    if ( (vc_start & 1) || (vc_start != vc_end))
        goto try_read;

    if (ret)
        return HT_FOUND;
    else
        return HT_NOT_FOUND;
}

static int
cuckoo_search(concurrent_ht_t *ht,
              size_t depth_start,
              size_t *insert_path)
{
    size_t depth = depth_start;

    while ((ht->kick_out < MAX_CUCKOO_COUNT) &&
           (depth < (MAX_CUCKOO_COUNT - 1))) {
        cuckoo_record *cur = ((cuckoo_record *)ht->cuckoo_path) + depth;
        cuckoo_record *next = ((cuckoo_record *)ht->cuckoo_path) + depth + 1;

        size_t idx;

        for (idx = 0; idx < NUM_CUCKOO_PATH; idx++) {
            size_t i, j;

            i = cur->buckets[idx];
            for (j = 0; j < BUCKET_SIZE; j++) {
                if (slot_is_empty(ht, i, j)) {
                    cur->slots[idx] = j;
                    *insert_path = idx;

                    return depth;
                }
            }

            /* Random pick up one to kick out */
            j = rand() % BUCKET_SIZE;
            cur->slots[idx] = j;
            ht_key_copy(&(cur->keys[idx]), &(TABLE_KEY(ht, i, j)));
            uint32_t hv = get_key_hash(cur->keys[idx].key,
                                       cur->keys[idx].key_len);
            next->buckets[idx] = get_second_index(ht, hv, i);
        }

        ht->kick_out++;
        depth++;
    }

    return -1;
}

static int
cuckoo_move(concurrent_ht_t *ht,
            size_t depth_start,
            size_t insert_path)
{
    size_t depth = depth_start;

    while (depth > 0) {
        cuckoo_record *from = ((cuckoo_record *)ht->cuckoo_path) + depth - 1;
        cuckoo_record *to = ((cuckoo_record *)ht->cuckoo_path) + depth;

        size_t bucket_from = from->buckets[insert_path];
        size_t slot_from = from->slots[insert_path];
        size_t bucket_to = to->buckets[insert_path];
        size_t slot_to = to->slots[insert_path];

        /*
         * NOTE that: There's a small change that the key we try to kick out
         * has been kicked out before in the previous cuckoo move operations.
         * So we just try again.
         *
         * This is due to the set associativity.
         */
        if ( !ht_key_equal(TABLE_KEY(ht, bucket_from, slot_from),
                           from->keys[insert_path]) ) {
            return depth;
        }

        MY_ASSERT(slot_is_empty(ht, bucket_to, slot_to));

        uint32_t hv = get_key_hash(TABLE_KEY(ht, bucket_to, slot_to).key,
                                   TABLE_KEY(ht, bucket_to, slot_to).key_len);
        size_t key_lock = keylock_index(hv);

        KVC_START_INCR(ht, key_lock);

        TABLE_TAG(ht, bucket_to, slot_to) =
                    TABLE_TAG(ht, bucket_from, slot_from);
        ht_key_copy(&(TABLE_KEY(ht, bucket_to, slot_to)),
                    &(TABLE_KEY(ht, bucket_from, slot_from)));
        ht_val_copy(&(TABLE_VAL(ht, bucket_to, slot_to)),
                    &(TABLE_VAL(ht, bucket_from, slot_from)));

        TABLE_TAG(ht, bucket_from, slot_from) = 0;
        TABLE_KEY(ht, bucket_from, slot_from).key_len = 0;
        TABLE_VAL(ht, bucket_from, slot_from).val_len = 0;

        KVC_END_INCR(ht, key_lock);

        depth--;
    }

    return depth;
}

static int
run_cuckoo(concurrent_ht_t *ht,
           size_t i1,
           size_t i2)
{
    int cur;
    size_t depth = 0, insert_path;

    ((cuckoo_record *)ht->cuckoo_path)[depth].buckets[0] = i1;
    ((cuckoo_record *)ht->cuckoo_path)[depth].buckets[1] = i2;
    ht->kick_out = 0;

    while (1) {
        cur = cuckoo_search(ht, depth, &insert_path);
        if (cur < 0)
            return -1;

        cur = cuckoo_move(ht, cur, insert_path);
        if (cur == 0)
            return insert_path;

        depth = cur - 1;
    }

    return -1;
}

static ht_status
cuckoo_insert(concurrent_ht_t *ht,
              const char *key,
              const char *val,
              uint32_t hv,
              size_t i1,
              size_t i2,
              uint32_t keylock)
{
    if (add_into_bucket(ht, key, val, hv, i1, keylock))
        return HT_INSERT_SUCCESS;

    if (add_into_bucket(ht, key, val, hv, i2, keylock))
        return HT_INSERT_SUCCESS;

    int insert_idx = run_cuckoo(ht, i1, i2);
    if (insert_idx >= 0) {
        size_t i;

        i = ((cuckoo_record *)ht->cuckoo_path)[0].buckets[insert_idx];

        if (add_into_bucket(ht, key, val, hv, i, keylock))
            return HT_INSERT_SUCCESS;
    }

    return HT_INSERT_FAILURE_TABLE_FULL;
}

static ht_status
cuckoo_delete(concurrent_ht_t *ht,
              const char *key,
              size_t i1,
              size_t i2,
              uint32_t keylock)
{
    if (delete_from_bucket(ht, key, i1, keylock))
        return HT_DELETE_SUCCESS;

    if (delete_from_bucket(ht, key, i2, keylock))
        return HT_DELETE_SUCCESS;

    return HT_DELETE_FAILURE_NOT_FOUND;
}

/******************************************************************/

concurrent_ht_t*
concur_hashtable_init(const size_t init_hashpower)
{
    size_t i, j, total_buckets;

    // allocation
#ifdef USE_NIC_MEMORY
    concurrent_ht_t *ht = (concurrent_ht_t *)nic_local_shared_mm_malloc(
            sizeof(concurrent_ht_t));
#else
    concurrent_ht_t *ht = (concurrent_ht_t *)malloc(sizeof(concurrent_ht_t));
#endif

    if ( !ht ) {
        printf("Could not malloc ht\n");
        goto err;
    }

    memset(ht, 0x00, sizeof(concurrent_ht_t));
    ht->hash_power = (init_hashpower > 0) ? init_hashpower : HASHPOWER_DEFAULT;
    ht->hash_total_items = 0;
    ht->kick_out = 0;

    total_buckets = HASH_SIZE(ht->hash_power);
#ifdef USE_NIC_MEMORY
    ht->buckets = nic_local_shared_mm_malloc(total_buckets * sizeof(ht_bucket_t)
            + total_buckets * BUCKET_SIZE * sizeof(ht_kv_t));
#else
    ht->buckets = malloc(total_buckets * sizeof(ht_bucket_t) +
            total_buckets * BUCKET_SIZE * sizeof(ht_kv_t));
#endif

    if ( !ht->buckets ) {
        printf("Could not malloc buckets\n");
        goto err;
    }

#ifdef USE_NIC_MEMORY
    ht->keyver_array = nic_local_shared_mm_malloc(KEY_VER_SIZE *
            sizeof(uint32_t));
#else
    ht->keyver_array = malloc(KEY_VER_SIZE * sizeof(uint32_t));
#endif

    if ( !ht->keyver_array ) {
        printf("Could not malloc keyver_array\n");
        goto err;
    }

#ifdef USE_NIC_MEMORY
    ht->cuckoo_path = nic_local_shared_mm_malloc(MAX_CUCKOO_COUNT *
            sizeof(cuckoo_record));
#else
    ht->cuckoo_path = malloc(MAX_CUCKOO_COUNT * sizeof(cuckoo_record));
#endif

    if ( !ht->cuckoo_path ) {
        printf("Could not malloc cuckoo_path\n");
        goto err;
    }

    // initialization
    mylock_init(ht);
    memset(ht->buckets, 0x00, total_buckets * sizeof(ht_bucket_t) +
                              total_buckets * BUCKET_SIZE * sizeof(ht_kv_t));
    memset(ht->keyver_array, 0x00, KEY_VER_SIZE * sizeof(uint32_t));
    memset(ht->cuckoo_path, 0x00, MAX_CUCKOO_COUNT * sizeof(cuckoo_record));

    ht_kv_t* kv_data_start = (ht_kv_t *)((ht_bucket_t *)ht->buckets +
                                         total_buckets);
    for (i = 0; i < total_buckets; i++) {
        for (j = 0; j < BUCKET_SIZE; j++) {
            TABLE_KV(ht, i, j) = kv_data_start + i * BUCKET_SIZE + j;
        }
    }

    return ht;

err:
    if (ht) {
#ifdef USE_NIC_MEMORY
        nic_local_shared_mm_free(ht->buckets);
        nic_local_shared_mm_free(ht->keyver_array);
        nic_local_shared_mm_free(ht->cuckoo_path);
#else
        free(ht->buckets);
        free(ht->keyver_array);
        free(ht->cuckoo_path);
#endif
    }

#ifdef USE_NIC_MEMORY
    nic_local_shared_mm_free(ht);
#else
    free(ht);
#endif

    return NULL;
}

void
concur_hashtable_free(concurrent_ht_t *ht)
{
    mylock_destroy(ht);
#ifdef USE_NIC_MEMORY
    nic_local_shared_mm_free(ht->buckets);
    nic_local_shared_mm_free(ht->keyver_array);
    nic_local_shared_mm_free(ht->cuckoo_path);
    nic_local_shared_mm_free(ht);
#else
    free(ht->buckets);
    free(ht->keyver_array);
    free(ht->cuckoo_path);
    free(ht);
#endif
}

ht_status
concur_hashtable_find(concurrent_ht_t *ht,
                      const char *key,
                      char *val)
{
    uint32_t hv = get_key_hash(key, strlen(key));
    size_t i1 = get_first_index(ht, hv);
    size_t i2 = get_second_index(ht, hv, i1);
    uint32_t key_lock = keylock_index(hv);

    ht_status ret;

    ret = cuckoo_find(ht, key, val, hv, i1, i2, key_lock);

    return ret;
}

ht_status
concur_hashtable_insert(concurrent_ht_t *ht,
                        const char *key,
                        const char *val)
{
    uint32_t hv = get_key_hash(key, strlen(key));
    size_t i1 = get_first_index(ht, hv);
    size_t i2 = get_second_index(ht, hv, i1);
    uint32_t key_lock = keylock_index(hv);

    ht_status ret;
    char old_val[VAL_SIZE];

    mylock_lock(ht);

    ret = cuckoo_find(ht, key, old_val, hv, i1, i2, key_lock);
    if (ret == HT_FOUND) {
        mylock_unlock(ht);
        return  HT_INSERT_FAILURE_KEY_DUPLICATED;
    }

    ret = cuckoo_insert(ht, key, val, hv, i1, i2, key_lock);

    mylock_unlock(ht);

    return ret;
}

ht_status
concur_hashtable_delete(concurrent_ht_t *ht,
                        const char *key)
{
    uint32_t hv = get_key_hash(key, strlen(key));
    size_t i1 = get_first_index(ht, hv);
    size_t i2 = get_second_index(ht, hv, i1);
    uint32_t key_lock = keylock_index(hv);

    ht_status ret;

    mylock_lock(ht);

    ret = cuckoo_delete(ht, key, i1, i2, key_lock);

    mylock_unlock(ht);

    return ret;
}

void
concur_hashtable_print_status (ht_status s)
{
    switch (s) {
        case HT_FOUND:
            printf("HT find success\n");
            break;

        case HT_NOT_FOUND:
            printf("HT find failed (not existed)\n");
            break;

        case HT_INSERT_SUCCESS:
            printf("HT insert success\n");
            break;

        case HT_INSERT_FAILURE_TABLE_FULL:
            printf("HT insert failed (table full)\n");
            break;

        case HT_INSERT_FAILURE_KEY_DUPLICATED:
            printf("HT insert failed (key duplicated)\n");
            break;

        case HT_DELETE_SUCCESS:
            printf("HT delete success\n");
            break;

        case HT_DELETE_FAILURE_NOT_FOUND:
            printf("HT delete failed (not existed)\n");
            break;

        default:
            printf("Unknow HT status code\n");
            break;
    }
}

void
concur_hashtable_report(concurrent_ht_t *ht)
{
    printf("***The concurrent hashtable information****\n");
    printf("Capacity: %u\n", HASH_SIZE(ht->hash_power) * BUCKET_SIZE);
    printf("Total saved items: %lu\n", ht->hash_total_items);

    printf("Load factor: %0.4lf\n", 1.0 * ht->hash_total_items / BUCKET_SIZE /
                                    HASH_SIZE(ht->hash_power));
}
