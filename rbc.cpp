//
// Created by avijeet on 7/2/24.
//

#include "zr/meta/simple.h"
#include "zr/ds/ring_buffer_ipc.h"

#include <x86intrin.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <fstream>
#include <random>
#include <utility>

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
    Data& at(uint64_t id) {
        return source[indices[id]];
    }

  private:
    std::vector<Data> source;
    std::vector<int> indices;
    static constexpr size_t count = 1024;
};

void dump_csv(std::vector<uint64_t>& push, std::vector<uint64_t>& pop) {
    std::ofstream dump("lag.csv");
    dump << "push,pop\n";
    for (int i = 0; std::cmp_less(i, push.size()); ++i) {
        dump << push[i] << ',' << pop[i] << '\n';
    }
}

void print_data_compare(Data& lhs, Data& rhs) {
    std::cout << lhs.cycles << ' ' << rhs.cycles << '\n';
    for (int i = 0; std::cmp_less(i, sizeof(Data::data) / sizeof(uint64_t)); ++i) {
        std::cout << lhs.data[0] << ' ' << rhs.data[0] << '\n';
    }
}

int main() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(14, &cpuset);
    int cpu_set_err = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (cpu_set_err != 0) {
        std::cerr << "Failed to set CPU affinity\n" << strerror(errno) << '\n';
        return EXIT_FAILURE;
    }
    rb_ipc_consumer<Data> rb("rb");
    uint64_t count = 3000000;
    std::vector<uint64_t> push_cycles, pop_cycles;
    push_cycles.reserve(count);
    pop_cycles.reserve(count);
    std::cout.imbue(std::locale(""));
    std::cout << "Count: " << count << '\n';
    Data data;
    int drop_count_precaution = 0;
    int drop_count = 0;
    int mis_read = 0;
    rb.catchup();
    RandomData rnd_source;
    auto start = std::chrono::steady_clock::now();
    while (count--) {
        volatile ring_buffer_read_status read_status;
        bool dropped = false;
        do {
            read_status = rb.pop(data);
            switch (read_status) {
                case ring_buffer_read_status::read_lapped_precaution:
                    drop_count_precaution++;
                    break;
                case ring_buffer_read_status::read_lapped:
                    drop_count++;
                    break;
            }
        } while (read_status != ring_buffer_read_status::read_new);
        pop_cycles.push_back(std::chrono::steady_clock::now().time_since_epoch().count());
        push_cycles.push_back(data.cycles);
    }
    auto end = std::chrono::steady_clock::now();
    std::cout << "Time elapsed: " << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << '\n';
    std::cout << "Lapped: " << drop_count << '\n';
    std::cout << "Lapped (Precaution): " << drop_count_precaution << '\n';
    std::cout << "Copies made: " << Data::count << '\n';
    dump_csv(push_cycles, pop_cycles);
}
