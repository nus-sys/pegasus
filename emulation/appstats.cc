#include "appstats.h"
#include "logger.h"

Stats::Stats()
    : total_ops(0), record(false)
{
}

Stats::Stats(const char *stats_file)
    : total_ops(0), record(false)
{
    if (stats_file != nullptr) {
        this->file_stream.open(stats_file, std::ofstream::out | std::ofstream::trunc);
        if (!this->file_stream) {
            panic("Failed to open stats output file");
        }
    }
}

Stats::~Stats()
{
    if (this->file_stream.is_open()) {
        this->file_stream.close();
    }
}

void
Stats::report_latency(int latency)
{
    if (this->record) {
        this->latencies[latency] += 1;
        this->total_ops += 1;
    }
}

void
Stats::start()
{
    gettimeofday(&this->start_time, nullptr);
    this->record = true;
}

void
Stats::done()
{
    gettimeofday(&this->end_time, nullptr);
    this->record = false;
}

void
Stats::dump()
{
    uint64_t duration = (this->end_time.tv_sec - this->start_time.tv_sec) * 1000000 +
        (this->end_time.tv_usec - this->start_time.tv_usec);
    uint64_t total_latency = 0, count = 0;
    int med_latency = -1, n_latency = -1, nn_latency = -1;
    for (auto latency : this->latencies) {
        total_latency += (latency.first * latency.second);
        count += latency.second;
        if (count >= this->total_ops / 2 && med_latency == -1) {
            med_latency = latency.first;
        }
        if (count >= (int)(this->total_ops * 0.9) && n_latency == -1) {
            n_latency = latency.first;
        }
        if (count >= (int)(this->total_ops * 0.99) && nn_latency == -1) {
            nn_latency = latency.first;
        }
    }

    printf("Throughput: %d\n", (int)(this->total_ops / ((float)duration / 1000000)));
    printf("Average Latency: %d\n", (int)(total_latency / this->total_ops));
    printf("Median Latency: %d\n", med_latency);
    printf("90%% Latency: %d\n", n_latency);
    printf("99%% Latency: %d\n", nn_latency);

    if (this->file_stream.is_open()) {
        for (auto latency : this->latencies) {
            this->file_stream << latency.first << " " << latency.second << std::endl;
        }
    }

    _dump();
}
