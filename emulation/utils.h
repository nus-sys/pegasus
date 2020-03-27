#ifndef _UTILS_H_
#define _UTILS_H_

#include <sched.h>

#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <logger.h>

inline int latency(const struct timeval &start, const struct timeval &end)
{
    return (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
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

inline void wait(const struct timeval &start, int time)
{
    struct timeval now;

    while (true) {
        gettimeofday(&now, nullptr);
        if (latency(start, now) >= time) {
            break;
        }
    }
}

inline void wait(int time)
{
    struct timeval start;
    gettimeofday(&start, nullptr);
    wait(start, time);
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
