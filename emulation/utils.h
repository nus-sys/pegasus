#ifndef _UTILS_H_
#define _UTILS_H_

#include <sched.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <logger.h>

inline void convert_endian(void *dst, const void *src, size_t size)
{
    uint8_t *dptr, *sptr;
    for (dptr = (uint8_t*)dst, sptr = (uint8_t*)src + size - 1;
         size > 0;
         size--) {
        *dptr++ = *sptr--;
    }
}

inline int latency(const struct timeval &start, const struct timeval &end)
{
    return (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
}

inline long latency_ns(const struct timespec &start, const struct timespec &end)
{
    return (end.tv_sec - start.tv_sec) * 1000000000L + (end.tv_nsec - start.tv_nsec);
}

inline struct timeval get_prev_timeval(const struct timeval &t, int interval)
{
    struct timeval ret;
    int64_t usec = t.tv_sec * 1000000 + t.tv_usec;
    usec -= interval;
    if (usec < 0) {
        panic("timer wrap-around not supported");
    }
    ret.tv_sec = usec / 1000000;
    ret.tv_usec = usec % 1000000;
    return ret;
}

inline int timeval_cmp(const struct timeval &t1, const struct timeval &t2)
{
    if (t1.tv_sec != t2.tv_sec) {
        return (int64_t)t1.tv_sec - (int64_t)t2.tv_sec;
    }
    return (int64_t)t1.tv_usec - (int64_t)t2.tv_usec;
}

inline void wait(struct timeval &t, int usec)
{
    struct timeval start = t;

    while (true) {
        gettimeofday(&t, nullptr);
        if (latency(start, t) >= usec) {
            break;
        }
    }
}

inline void wait_ns(struct timespec &ts, long nsec)
{
    struct timespec start = ts;

    while (true) {
        clock_gettime(CLOCK_REALTIME, &ts);
        if (latency_ns(start, ts) >= nsec) {
            break;
        }
    }
}

inline void wait(int usec)
{
    struct timeval start;
    gettimeofday(&start, nullptr);
    wait(start, usec);
}

inline void pin_to_core(int core_id) {
    if (core_id >= 0) {
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        CPU_SET(core_id, &cpu_set);
        if (sched_setaffinity(0, sizeof(cpu_set), &cpu_set) != 0) {
            panic("Failed to set cpu affinity\n");
        }
    }
}

#endif /* _UTILS_H_ */
