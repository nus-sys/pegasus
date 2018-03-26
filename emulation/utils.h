#ifndef __UTILS_H__
#define __UTILS_H__

#include <sys/time.h>
#include <unistd.h>

inline int latency(const struct timeval &start, const struct timeval &end) {
    return (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
}

inline void wait(const struct timeval &start, int time) {
    struct timeval now;

    while (true) {
        gettimeofday(&now, nullptr);
        if (latency(start, now) >= time) {
            break;
        }
    }
}

#endif /* __UTILS_H__ */
