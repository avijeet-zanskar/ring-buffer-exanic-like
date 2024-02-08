//
// Created by Avijeet on 01/02/2023
//

#ifndef ZR_DS_RING_BUFFER_IPC_H
#define ZR_DS_RING_BUFFER_IPC_H

#include "zr/meta/simple.h"
#include "zr/util/debug.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <linux/memfd.h>

#include <atomic>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <format>
#include <string>
#include <string_view>

enum class ring_buffer_ipc_type {
    Producer,
    Consumer
};

// single producer, multi consumer ring buffer for interprocess communication using shared memory
template <typename T, ring_buffer_ipc_type Type>
class ring_buffer_ipc {
    template <typename U>
    friend class rb_ipc_producer;
    template <typename U>
    friend class rb_ipc_consumer;
    struct block {
        block() : m_version(0) {
        }
        block(uint64_t version, const T& data) : m_version(version), data(data) {
        }
        alignas(64) uint64_t m_version;
        alignas(64) T data;
    };
    struct info {
        info() : last_block_id(0) {
        }
        uint64_t last_block_id;
    };
    struct file_descriptor {
        char rb[128], info[128];
    };

    ring_buffer_ipc(std::string_view name);
    ~ring_buffer_ipc();

    void push(const T& data);
    const ring_buffer_ipc<T, Type>::block& pop_at(uint64_t id);

    static constexpr uint64_t m_capacity = 4 * 1024;

    std::string m_name;
    file_descriptor* m_fd;
    block* m_buffer;
    info* m_info;
    uint64_t m_version;
};

template <typename T>
class rb_ipc_producer {
  public:
    rb_ipc_producer(std::string_view name);

    void push(const T& data);
    uint64_t capacity();

  private:
    ring_buffer_ipc<T, ring_buffer_ipc_type::Producer> rb;
};

enum class ring_buffer_read_status {
    read_new,
    read_lapped,
    read_lapped_precaution,
    read_no_new
};

template <typename T>
class rb_ipc_consumer {
  public:
    rb_ipc_consumer(std::string_view name);

    void catchup();
    ring_buffer_read_status pop(T& data);
    uint64_t capacity();
    uint64_t id;

  private:
    ring_buffer_ipc<T, ring_buffer_ipc_type::Consumer> rb;
    uint64_t m_prev_id, m_prev_version, m_version;
};

template <typename T, ring_buffer_ipc_type Type>
inline ring_buffer_ipc<T, Type>::ring_buffer_ipc(std::string_view name)
    : m_name(name), m_fd(nullptr), m_buffer(nullptr), m_info(nullptr), m_version(0) {
    // create shared memory for sharing file descriptors with consumers
    int fd;
    if constexpr (Type == ring_buffer_ipc_type::Producer) {
        fd = shm_open(std::format("{}_rb_fd", name).c_str(), O_CREAT | O_RDWR, S_IRWXU);
    } else {
        fd = shm_open(std::format("{}_rb_fd", name).c_str(), O_RDWR, S_IREAD);
    }
    if (fd == -1) {
        ZR_ABORT(std::format("shm_open failed.\nError: {}", std::strerror(errno)));
    }

    int err = ftruncate(fd, sizeof(file_descriptor));
    if (err == -1) {
        ZR_ABORT(std::format("ftruncate failed.\nError: {}", std::strerror(errno)));
    }

    void* mapping;
    if constexpr (Type == ring_buffer_ipc_type::Producer) {
        mapping = mmap(nullptr, sizeof(file_descriptor), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    } else {
        mapping = mmap(nullptr, sizeof(file_descriptor), PROT_READ, MAP_SHARED, fd, 0);
    }
    if (mapping == MAP_FAILED) {
        ZR_ABORT(std::format("mmap failed.\nError: {}", std::strerror(errno)));
    }

    m_fd = static_cast<file_descriptor*>(mapping);
    ZR_ASSERT(m_fd != nullptr);
}

template <typename T, ring_buffer_ipc_type Type>
inline ring_buffer_ipc<T, Type>::~ring_buffer_ipc() {
    if constexpr (Type == ring_buffer_ipc_type::Producer) {
        int res = munmap(m_fd, sizeof(file_descriptor));
        if (res == -1) {
            ZR_ABORT(std::format("munmap failed.\nError: {}", std::strerror(errno)));
        }
        res = shm_unlink(std::format("{}_rb_fd", m_name).c_str());
        if (res == -1) {
            ZR_ABORT(std::format("shm_unlink failed.\nError: {}", std::strerror(errno)));
        }
    }
}

template <typename T, ring_buffer_ipc_type Type>
inline void ring_buffer_ipc<T, Type>::push(const T& data) {
    uint64_t next_block_id = m_info->last_block_id + 1;
    uint64_t version = m_version += (next_block_id % m_capacity == 0 ? 1 : 0);
    m_buffer[next_block_id % m_capacity] = block(version, data);
    m_info->last_block_id = next_block_id;
}

template <typename T, ring_buffer_ipc_type Type>
inline const ring_buffer_ipc<T, Type>::block& ring_buffer_ipc<T, Type>::pop_at(uint64_t id) {
    return m_buffer[id % m_capacity];
}

template <typename T>
inline rb_ipc_producer<T>::rb_ipc_producer(std::string_view name) : rb(name) {
    int fd = memfd_create("rb", MFD_HUGETLB | MFD_HUGE_2MB);
    if (fd == -1) {
        ZR_ABORT(std::format("memfd_create failed.\nError: {}", std::strerror(errno)));
    }
    std::string fd_path = std::format("/proc/{}/fd/{}", getpid(), fd);
    ZR_ASSERT(fd_path.size() < 128);
    std::memcpy(rb.m_fd->rb, fd_path.c_str(), fd_path.size());

    void* mapping = mmap(nullptr,
                         rb.m_capacity * sizeof(typename decltype(rb)::block),
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_HUGETLB,
                         fd,
                         0);
    if (mapping == MAP_FAILED) {
        ZR_ABORT(std::format("mmap failed.\nError: {}", std::strerror(errno)));
    }
    rb.m_buffer = new (mapping) typename decltype(rb)::block[rb.m_capacity];

    fd = memfd_create("info", 0);
    if (fd == -1) {
        ZR_ABORT(std::format("memfd_create failed.\nError: {}", std::strerror(errno)));
    }
    fd_path = std::format("/proc/{}/fd/{}", getpid(), fd);
    ZR_ASSERT(fd_path.size() < 128);
    std::memcpy(rb.m_fd->info, fd_path.c_str(), fd_path.size());

    int err = ftruncate(fd, sizeof(typename decltype(rb)::info));
    if (err == -1) {
        ZR_ABORT(std::format("ftruncate failed.\nError: {}", std::strerror(errno)));
    }

    mapping = mmap(nullptr, sizeof(typename decltype(rb)::info), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        ZR_ABORT(std::format("mmap failed.\nError: {}", std::strerror(errno)));
    }

    rb.m_info = new (mapping) typename decltype(rb)::info();

    ZR_ASSERT(rb.m_buffer != nullptr);
    ZR_ASSERT(rb.m_info != nullptr);
}

template <typename T>
inline void rb_ipc_producer<T>::push(const T& data) {
    rb.push(data);
}

template <typename T>
inline uint64_t rb_ipc_producer<T>::capacity() {
    return rb.m_capacity;
}

template <typename T>
inline rb_ipc_consumer<T>::rb_ipc_consumer(std::string_view name) : rb(name) {
    int fd = open(rb.m_fd->rb, O_RDONLY);
    if (fd == -1) {
        ZR_ABORT(std::format("open failed. fd: {}\nError: {}", rb.m_fd->rb, std::strerror(errno)));
    }

    void* mapping = mmap(nullptr,
                         rb.m_capacity * sizeof(typename decltype(rb)::block),
                         PROT_READ,
                         MAP_SHARED | MAP_HUGETLB,
                         fd,
                         0);
    if (mapping == MAP_FAILED) {
        ZR_ABORT(std::format("mmap failed.\nError: {}", std::strerror(errno)));
    }
    rb.m_buffer = static_cast<typename decltype(rb)::block*>(mapping);

    fd = open(rb.m_fd->info, O_RDONLY);
    if (fd == -1) {
        ZR_ABORT(std::format("open failed. fd: {}\nError: {}", rb.m_fd->info, std::strerror(errno)));
    }

    mapping = mmap(nullptr, sizeof(typename decltype(rb)::info), PROT_READ, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        ZR_ABORT(std::format("mmap failed.\nError: {}", std::strerror(errno)));
    }
    rb.m_info = static_cast<decltype(rb)::info*>(mapping);

    ZR_ASSERT(rb.m_buffer != nullptr);
    ZR_ASSERT(rb.m_info != nullptr);
}

template <typename T>
inline void rb_ipc_consumer<T>::catchup() {
    uint64_t break_id = 0, break_version = 0;
    for (uint64_t chunk = rb.m_capacity - 1; chunk > 0; --chunk) {
        const typename decltype(rb)::block block = rb.pop_at(chunk);
        if (chunk == rb.m_capacity - 1) {
            break_version = block.m_version;
            break_id = chunk;
        } else if (block.m_version != break_version) {
            break_version = block.m_version;
            break_id = chunk;
            break;
        }
    }

    m_prev_id = break_id;
    m_prev_version = break_version;
    id = break_id + 1;
    m_version = break_version;

    if (id % rb.m_capacity == 0) {
        m_version++;
    }
}

template <typename T>
inline ring_buffer_read_status rb_ipc_consumer<T>::pop(T& data) {
    const typename decltype(rb)::block& block = rb.pop_at(id);
    if (block.m_version == m_version) {
        uint64_t prev_version = m_prev_version;
        uint64_t prev_id = m_prev_id;

        m_prev_version = m_version;
        m_prev_id = id;

        id = id + 1;
        if (id % rb.m_capacity == 0) {
            ++m_version;
        }

        data = block.data;
        if (rb.pop_at(prev_id).m_version != prev_version) {
            catchup();
            return ring_buffer_read_status::read_lapped_precaution;
        } else {
            return ring_buffer_read_status::read_new;
        }
    } else if (block.m_version == m_version - 1) {
        //__builtin_prefetch((const void*)&rb.pop_at(m_prev_id).m_version, 0, 3);
        return ring_buffer_read_status::read_no_new;
    } else {
        catchup();
        return ring_buffer_read_status::read_lapped;
    }
}

template <typename T>
inline uint64_t rb_ipc_consumer<T>::capacity() {
    return rb.m_capacity;
}

#endif