//
// Created by avijeet on 7/2/24.
//

#include "zr/meta/simple.h"
#include "zr/ds/ring_buffer_ipc.h"

#include <cpuid.h>
#include <csignal>
#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>
#include <vector>

double get_tsc_freq() {
    unsigned int eax_denominator{}, ebx_numerator{}, ecx_hz{}, edx{};
    __get_cpuid(0x15, &eax_denominator, &ebx_numerator, &ecx_hz, &edx);
    return (static_cast<double>(ecx_hz) * ebx_numerator) / (1e9 * static_cast<double>(eax_denominator));
}

volatile bool exit_flag = false;

void handle_interrupt(int) {
    exit_flag = true;
}

struct Data {
    alignas(64) uint64_t cycles;
    alignas(64) uint64_t data[128];
    Data() = default;
    const Data& operator=(const Data& rhs) {
        std::memcpy(this, &rhs, sizeof(Data));
        ++count;
        return *this;
    }
    Data(const Data& rhs) {
        std::memcpy(this, &rhs, sizeof(Data));
        ++count;
    }
    static uint64_t count;
};

uint64_t Data::count = 0;

class RandomData {
  public:
    RandomData() {
        std::mt19937 rng(0);
        std::uniform_int_distribution<uint64_t> dist;
        source.resize(count);
        for (auto& d : source) {
            for (auto& val : d.data) {
                val = dist(rng);
            }
        }
        indices.resize(count);
        std::iota(indices.begin(), indices.end(), 0);
        std::shuffle(indices.begin(), indices.end(), rng);
    }
    Data& next() {
        return source[indices[id++ % count]];
    }

  private:
    std::vector<Data> source;
    std::vector<int> indices;
    static constexpr size_t count = 1024;
    uint64_t id = 2;
};

Data& next_data() {
    static RandomData rnd_source;
    Data& data = rnd_source.next();
    data.cycles = std::chrono::steady_clock::now().time_since_epoch().count();
    return data;
}

int main() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(13, &cpuset);
    int cpu_set_err = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (cpu_set_err != 0) {
        std::cerr << "Failed to set CPU affinity\n";
        return EXIT_FAILURE;
    }
    rb_ipc_producer<Data> rb("rb");
    signal(SIGINT, &handle_interrupt);
    signal(SIGTERM, &handle_interrupt);
    int cyc = static_cast<int>(1000 * get_tsc_freq());
    uint64_t data_count = 0;
    unsigned int temp;
    auto start = __rdtscp(&temp);
    auto start_time = std::chrono::steady_clock::now();
    while (!exit_flag) {
        if (auto end = __rdtscp(&temp); std::cmp_less(end - start, cyc)) {
            continue;
        } else {
            start = end;
            rb.push(next_data());
            ++data_count;
        }
    }
    auto end_time = std::chrono::steady_clock::now();
    std::cout << "Producer exit\n";
    std::cout.imbue(std::locale(""));
    std::cout << "Time elapsed: " << std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count()
              << '\n';
    std::cout << "Packets sent: " << data_count << '\n';
    std::cout << "Time per packet: " << (end_time - start_time).count() / data_count << '\n';
    std::cout << "Copies made: " << Data::count << '\n';
}