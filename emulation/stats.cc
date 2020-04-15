#include <cassert>

#include <stats.h>
#include <logger.h>
#include <utils.h>

#define DEFAULT_LATENCY 100

Stats::Stats()
    : issued_ops(0), completed_ops(0), interval(0)
{
}

Stats::Stats(const char *stats_file, int interval, const char *interval_file)
    : issued_ops(0), completed_ops(0), interval(interval)
{
    if (stats_file != nullptr) {
        this->file_stream.open(stats_file, std::ofstream::out | std::ofstream::trunc);
        if (!this->file_stream) {
            panic("Failed to open stats output file");
        }
    }
    if (interval_file != nullptr) {
        this->interval_file = std::string(interval_file);
    }
}

Stats::~Stats()
{
    if (this->file_stream.is_open()) {
        this->file_stream.close();
    }
}

void Stats::report_issue()
{
    std::atomic_fetch_add(&this->issued_ops, 1);
}

void Stats::report_latency(int l)
{
    std::lock_guard<std::mutex> lck(this->mtx);
    this->latencies[l]++;
    this->completed_ops++;
    if (this->interval > 0) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        if (latency(this->last_interval, tv) >= this->interval) {
            this->last_interval = tv;
            this->interval_latencies.push_back(std::map<int, uint64_t>());
        }
        this->interval_latencies.back()[l]++;
    }
}

int Stats::get_latency(float percentile)
{
    std::lock_guard<std::mutex> lck(this->mtx);
    uint64_t count = 0;

    assert(percentile >= 0 && percentile < 1);
    if (this->latencies.empty()) {
        return DEFAULT_LATENCY;
    }
    for (const auto &latency : this->latencies) {
        count += latency.second;
        if (count >= (uint64_t)(this->completed_ops * percentile)) {
            return latency.first;
        }
    }
    panic("Unreachable");
}

void Stats::start()
{
    gettimeofday(&this->start_time, nullptr);
    if (this->interval > 0) {
        this->last_interval = this->start_time;
        this->interval_latencies.push_back(std::map<int, uint64_t>());
    }
}

void Stats::done()
{
    gettimeofday(&this->end_time, nullptr);
}

void Stats::dump()
{
    uint64_t duration = (this->end_time.tv_sec - this->start_time.tv_sec) * 1000000 +
        (this->end_time.tv_usec - this->start_time.tv_usec);
    uint64_t total_latency = 0, count = 0;
    int med_latency = -1, n_latency = -1, nn_latency = -1;
    for (const auto &latency : this->latencies) {
        total_latency += (latency.first * latency.second);
        count += latency.second;
        if (count >= this->completed_ops / 2 && med_latency == -1) {
            med_latency = latency.first;
        }
        if (count >= (uint64_t)(this->completed_ops * 0.9) && n_latency == -1) {
            n_latency = latency.first;
        }
        if (count >= (uint64_t)(this->completed_ops * 0.99) && nn_latency == -1) {
            nn_latency = latency.first;
        }
    }

    printf("Throughput: %d\n", (int)(this->completed_ops / ((float)duration / 1000000)));
    printf("Completed ops: %lu, issued ops: %llu\n", this->completed_ops, std::atomic_load(&this->issued_ops));
    printf("Average Latency: %d\n", (int)(total_latency / this->completed_ops));
    printf("Median Latency: %d\n", med_latency);
    printf("90%% Latency: %d\n", n_latency);
    printf("99%% Latency: %d\n", nn_latency);

    if (this->file_stream.is_open()) {
        this->file_stream << this->completed_ops << " " << std::atomic_load(&this->issued_ops) << std::endl;
        for (auto latency : this->latencies) {
            this->file_stream << latency.first << " " << latency.second << std::endl;
        }
        this->file_stream.close();
    }

    if (this->interval > 0) {
        if (this->interval_file.size() > 0) {
            for (unsigned int i = 0; i < this->interval_latencies.size(); i++) {
                std::ofstream fs;
                std::string fname = this->interval_file;
                fname += std::to_string(i);
                fs.open(fname.c_str(), std::ofstream::out | std::ofstream::trunc);
                if (!fs) {
                    printf("Failed to open file %s\n", fname.c_str());
                    break;
                }
                for (auto latency : this->interval_latencies[i]) {
                    fs << latency.first << " " << latency.second << std::endl;
                }
                fs.close();
            }
        }
    }

    _dump();
}

void Stats::_dump()
{
}
