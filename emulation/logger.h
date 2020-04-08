#ifndef _LOGGER_H_
#define _LOGGER_H_

enum LogLevel : int {
    LOG_NONE = 0,
    LOG_ERROR,
    LOG_INFO,
    LOG_DEBUG
};

void logger_init(LogLevel level);

void debug(const char *fmt, ...);
void info(const char *fmt, ...);
void error(const char *fmt, ...);
void panic(const char *fmt, ...) __attribute__ ((noreturn));

#endif /* _LOGGER_H_ */
