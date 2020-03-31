#include <stdio.h>
#include <stdlib.h>

#include <logger.h>

void panic(const char *msg)
{
    printf("PANIC: %s\n", msg);
    exit(1);
}
