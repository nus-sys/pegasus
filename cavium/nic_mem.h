/*
 * OCTEON shared memory header
 */
#ifndef _NIC_MEM_H_
#define _NIC_MEM_H_

#define ALIGNMENT 128
#define MEM_SIZE (1024*1024*1024)

void* nic_local_shared_mm;
void  nic_local_shared_mm_init();
void* nic_local_shared_mm_malloc(int size);
void* nic_local_shared_mm_memalign(int size, int alignment);
void* nic_local_shared_mm_realloc(void *ptr, int size);
void nic_local_shared_mm_free(void *ptr);

#endif /* _NIC_MEM_H_ */
