float wait_ticks(long ticks)
{
    float res = 1.5;
    for (long i = 0; i < ticks; i++) {
        res *= 3.7;
    }
    return res;
}

