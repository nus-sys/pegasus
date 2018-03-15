#ifndef __UTILS_H__
#define __UTILS_H__

#include <sys/time.h>
#include <unistd.h>

inline int latency(const struct timeval &start, const struct timeval &end) {
    return (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
}

void wait(int time) {
    struct timeval start, now;
    gettimeofday(&start, nullptr);

    while (true) {
        gettimeofday(&now, nullptr);
        if (latency(start, now) >= time) {
            break;
        }
    }
}

#endif /* __UTILS_H__ */
