/**
 * @file ringfire.h
 * @brief Zero-copy lock-free Inter-Process Communication (IPC) ring buffer and shared memory bus.
 *
 * Compatible with C11 and C++11 onwards.
 */

#ifndef RINGFIRE_H
#define RINGFIRE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RINGFIRE_MAGIC 0x52494E4746495245ULL // "RINGFIRE"
#define RINGFIRE_VERSION 1U

#define BLACKBOARD_MAGIC 0x52494E4742424F52ULL // "RINGBBOR"
#define BLACKBOARD_VERSION 1U

#define RINGFIRE_FLAG_POLICY_LATEST_WINS 0x0001
#define RINGFIRE_FLAG_MODE_SPMC          0x0010
#define RINGFIRE_FLAG_MODE_MPMC          0x0020

/**
 * @brief 128-byte aligned header placed at the beginning of the shared memory region.
 */
typedef struct __attribute__((aligned(128))) {
    uint64_t magic;              // Magic signature ("RINGFIRE")
    uint32_t version;            // Protocol version (1)
    uint32_t element_size;        // Fixed Slot size in bytes
    uint64_t capacity;            // Number of slots (power of 2)
    uint64_t mask;                // capacity - 1
    _Atomic uint64_t write_seq;   // Highest published sequence
    _Atomic uint64_t claim_seq;   // Highest claimed sequence (MPMC)
    uint32_t flags;               // Operational flags
    _Atomic uint32_t futex_word;  // Futex notification word
    _Atomic uint32_t waiting_consumers; // Sleeping consumers count
    uint32_t _align_pad;          // Alignment padding
    _Atomic uint64_t read_seq;    // Atomic read sequence for MPMC queue
    uint64_t _reserved;           // Reserved
    uint8_t _pad[48];             // Cache-line padding to 128 bytes
} ringfire_header_t;

/**
 * @brief 128-byte aligned header placed at the beginning of the Blackboard shared memory region.
 */
typedef struct __attribute__((aligned(128))) {
    uint64_t magic;              // Magic signature ("RINGBBOR")
    uint32_t version;            // Protocol version (1)
    uint32_t value_size;          // Value size in bytes
    uint32_t slot_size;           // BlackboardSlot size in bytes (64-byte aligned)
    uint32_t slot_count;          // Number of slots
    uint64_t _reserved[2];        // Reserved
    uint8_t _pad[88];             // Cache-line padding to 128 bytes
} ringfire_blackboard_header_t;

/* Dynamic opaque handle definitions for Rust FFI */
typedef struct RingProducerRaw ringfire_producer_t;
typedef struct RingConsumerRaw ringfire_consumer_t;
typedef struct BlackboardRaw ringfire_blackboard_t;

/* ========================================================================= */
/* Rust Shared Library FFI Prototypes                                        */
/* ========================================================================= */

ringfire_producer_t* ringfire_producer_create(const char* path, uint64_t capacity, uint32_t element_size);
int ringfire_producer_push(ringfire_producer_t* prod, const void* data);
void ringfire_producer_close(ringfire_producer_t* prod);

ringfire_consumer_t* ringfire_consumer_attach(const char* path, uint32_t element_size);
int ringfire_consumer_try_recv(ringfire_consumer_t* cons, void* out_data);
size_t ringfire_consumer_recv_batch(ringfire_consumer_t* cons, void* out_buf, size_t max_count);
void ringfire_consumer_close(ringfire_consumer_t* cons);

ringfire_blackboard_t* ringfire_blackboard_create(const char* path, size_t slot_count, size_t value_size);
ringfire_blackboard_t* ringfire_blackboard_attach(const char* path, size_t value_size);
int ringfire_blackboard_write(ringfire_blackboard_t* bb, size_t key, const void* data);
int ringfire_blackboard_read(const ringfire_blackboard_t* bb, size_t key, void* out_data);
void ringfire_blackboard_close(ringfire_blackboard_t* bb);

/* ========================================================================= */
/* Standalone Header-Only Inline C API                                       */
/* ========================================================================= */

typedef struct {
    int fd;
    void* mmap_ptr;
    size_t total_size;
    ringfire_header_t* header;
    uint8_t* slots_base;
    uint64_t mask;
    uint64_t cursor;
    uint32_t element_size;
    uint32_t slot_stride;
} ringfire_c_consumer_t;

/**
 * @brief Attach to an existing ring buffer from pure C without linking any library.
 */
static inline int ringfire_c_consumer_attach(ringfire_c_consumer_t* cons, const char* path, uint32_t element_size) {
    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }

    void* ptr = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        close(fd);
        return -1;
    }

    ringfire_header_t* header = (ringfire_header_t*)ptr;
    if (header->magic != RINGFIRE_MAGIC || header->version != RINGFIRE_VERSION) {
        munmap(ptr, (size_t)st.st_size);
        close(fd);
        return -2;
    }

    uint32_t slot_stride = ((8 + element_size + 7) / 8) * 8;
    if (header->element_size != slot_stride) {
        munmap(ptr, (size_t)st.st_size);
        close(fd);
        return -3;
    }

    cons->fd = fd;
    cons->mmap_ptr = ptr;
    cons->total_size = (size_t)st.st_size;
    cons->header = header;
    cons->slots_base = (uint8_t*)ptr + sizeof(ringfire_header_t);
    cons->mask = header->mask;
    cons->element_size = element_size;
    cons->slot_stride = slot_stride;

    uint64_t write_seq = atomic_load_explicit(&header->write_seq, memory_order_acquire);
    cons->cursor = (write_seq > header->capacity) ? (write_seq - header->capacity + 1) : 1;

    return 0;
}

/**
 * @brief Zero-copy try_recv in pure C. Returns 1 if received, 0 if empty.
 */
static inline int ringfire_c_consumer_try_recv(ringfire_c_consumer_t* cons, void* out_data) {
    uint64_t idx = cons->cursor & cons->mask;
    uint8_t* slot = cons->slots_base + (idx * cons->slot_stride);
    _Atomic uint64_t* seq_ptr = (_Atomic uint64_t*)slot;

    uint64_t s1 = atomic_load_explicit(seq_ptr, memory_order_acquire);
    if (s1 < cons->cursor) {
        return 0; // Empty
    }

    if (s1 > cons->cursor) {
        cons->cursor = s1; // Lapped by producer
    }

    memcpy(out_data, slot + 8, cons->element_size);

    uint64_t s2 = atomic_load_explicit(seq_ptr, memory_order_acquire);
    if (s1 != s2) {
        cons->cursor = s2;
        return 0; // Overwritten during read, retry next
    }

    cons->cursor++;
    return 1;
}

static inline void ringfire_c_consumer_detach(ringfire_c_consumer_t* cons) {
    if (cons->mmap_ptr && cons->mmap_ptr != MAP_FAILED) {
        munmap(cons->mmap_ptr, cons->total_size);
        cons->mmap_ptr = NULL;
    }
    if (cons->fd >= 0) {
        close(cons->fd);
        cons->fd = -1;
    }
}

#ifdef __cplusplus
}
#endif

#endif /* RINGFIRE_H */
