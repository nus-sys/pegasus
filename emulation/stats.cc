#include <cassert>

#include <stats.h>
#include <logger.h>
#include <utils.h>

Stats::ThreadStats::ThreadStats()
    : issued_ops(0), completed_ops(0)
{
}

Stats::Stats(int n_threads,
             const char *stats_file,
             int interval,
             const char *interval_file)
    : interval(interval)
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
    for (int i = 0; i < n_threads; i++) {
        this->thread_stats.push_back(new ThreadStats);
    }
}

Stats::~Stats()
{
    if (this->file_stream.is_open()) {
        this->file_stream.close();
    }
    for (ThreadStats *ts : this->thread_stats) {
        delete ts;
    }
}

void Stats::report_issue(int tid)
{
    this->thread_stats[tid]->issued_ops++;
}

void Stats::report_latency(int tid, int l)
{
    this->thread_stats[tid]->latencies[l]++;
    this->thread_stats[tid]->completed_ops++;
    if (this->interval > 0) {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        if (latency(this->thread_stats[tid]->last_interval, tv) >= this->interval) {
            this->thread_stats[tid]->last_interval = tv;
            this->thread_stats[tid]->interval_latencies.push_back(std::map<int, uint64_t>());
        }
        this->thread_stats[tid]->interval_latencies.back()[l]++;
    }
}

int Stats::get_latency(int tid, float percentile)
{
    uint64_t count = 0;

    assert(percentile >= 0 && percentile < 1);
    if (this->thread_stats[tid]->latencies.empty()) {
        return -1;
    }
    for (const auto &latency : this->thread_stats[tid]->latencies) {
        count += latency.second;
        if (count >= (uint64_t)(this->thread_stats[tid]->completed_ops * percentile)) {
            return latency.first;
        }
    }
    panic("Unreachable");
}

void Stats::start()
{
    gettimeofday(&this->start_time, nullptr);
    if (this->interval > 0) {
        for (ThreadStats *ts : this->thread_stats) {
            ts->last_interval = this->start_time;
            ts->interval_latencies.push_back(std::map<int, uint64_t>());
        }
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
    std::map<int, uint64_t> combined_latencies;
    uint64_t combined_issued_ops = 0, combined_completed_ops = 0;

    // Combine latency from all threads
    for (ThreadStats *ts : this->thread_stats) {
        for (const auto &latency : ts->latencies) {
            combined_latencies[latency.first] += latency.second;
        }
        combined_issued_ops += ts->issued_ops;
        combined_completed_ops += ts->completed_ops;
    }

    // Calculate latency stats
    for (const auto &latency : combined_latencies) {
        total_latency += (latency.first * latency.second);
        count += latency.second;
        if (count >= combined_completed_ops / 2 && med_latency == -1) {
            med_latency = latency.first;
        }
        if (count >= (uint64_t)(combined_completed_ops * 0.9) && n_latency == -1) {
            n_latency = latency.first;
        }
        if (count >= (uint64_t)(combined_completed_ops * 0.99) && nn_latency == -1) {
            nn_latency = latency.first;
        }
    }

    printf("Throughput: %d\n", (int)(combined_completed_ops / ((float)duration / 1000000)));
    printf("Completed ops: %lu, issued ops: %lu\n", combined_completed_ops, combined_issued_ops);
    printf("Average Latency: %d\n", (int)(total_latency / combined_completed_ops));
    printf("Median Latency: %d\n", med_latency);
    printf("90%% Latency: %d\n", n_latency);
    printf("99%% Latency: %d\n", nn_latency);

    if (this->file_stream.is_open()) {
        this->file_stream << combined_completed_ops << " " << combined_issued_ops << std::endl;
        for (auto latency : combined_latencies) {
            this->file_stream << latency.first << " " << latency.second << std::endl;
        }
        this->file_stream.close();
    }

    if (this->interval > 0) {
        if (this->interval_file.size() > 0) {
            std::vector<std::map<int, uint64_t>> combined_il;
            for (ThreadStats *ts : this->thread_stats) {
                std::vector<std::map<int, uint64_t>> &thread_il = ts->interval_latencies;
                if (thread_il.size() > combined_il.size()) {
                    combined_il.resize(thread_il.size());
                }
                for (unsigned int i = 0; i < thread_il.size(); i++) {
                    for (const auto &latency : thread_il[i]) {
                        combined_il[i][latency.first] += latency.second;
                    }
                }
            }
            for (unsigned int i = 0; i < combined_il.size(); i++) {
                std::ofstream fs;
                std::string fname = this->interval_file;
                fname += std::to_string(i);
                fs.open(fname.c_str(), std::ofstream::out | std::ofstream::trunc);
                if (!fs) {
                    panic("Failed to open file %s", fname.c_str());
                }
                for (auto latency : combined_il[i]) {
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
