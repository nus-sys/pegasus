#include "cvmx.h"
#include "cvmx-malloc.h"
#include "cvmx-bootmem.h"
#include "nic_mem.h"

CVMX_SHARED cvmx_arena_list_t nic_arena = NULL;

void
nic_local_shared_mm_init()
{
    int ret;
    nic_local_shared_mm = cvmx_bootmem_alloc_named(MEM_SIZE, 128, "nic_local_shared_mm");
    assert(nic_local_shared_mm);

    ret = cvmx_add_arena(&nic_arena, nic_local_shared_mm, MEM_SIZE);
    assert(ret == 0);
}

void*
nic_local_shared_mm_malloc(int size)
{
    return cvmx_malloc(nic_arena, size);
}

void*
nic_local_shared_mm_realloc(void *ptr, int size)
{
    return cvmx_realloc(nic_arena, ptr, size);
}

void*
nic_local_shared_memalign(int size, int alignment)
{
    return cvmx_memalign(nic_arena, alignment, size);
}

void
nic_local_shared_mm_free(void *ptr)
{
    cvmx_free(ptr);
}
