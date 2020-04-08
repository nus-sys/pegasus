#include <cstdio>
#include <cstdarg>
#include <cstdlib>

#include <logger.h>

static LogLevel current_level = LOG_INFO;

static void log(LogLevel level, const char *prefix, const char *fmt, va_list args);

static void log(LogLevel level, const char *prefix, const char *fmt, va_list args)
{
    if (level <= current_level) {
        printf("[%s]", prefix);
        vprintf(fmt, args);
        printf("\n");
    }
}

void logger_init(LogLevel level)
{
    current_level = level;
}

void debug(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    log(LOG_DEBUG, "DEBUG", fmt, args);
}

void info(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    log(LOG_INFO, "INFO", fmt, args);
}

void error(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    log(LOG_ERROR, "ERROR", fmt, args);
}

void panic(const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    log(LOG_NONE, "PANIC", fmt, args);
    exit(1);
}
