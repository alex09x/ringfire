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
#define RINGFIRE_VERSION 2U

/* Slot sequence value while a writer overwrites the slot payload (protocol v2). */
#define RINGFIRE_SLOT_WRITING UINT64_MAX

#define BLACKBOARD_MAGIC 0x52494E4742424F52ULL // "RINGBBOR"
#define BLACKBOARD_VERSION 1U

#define RINGFIRE_FLAG_POLICY_LATEST_WINS        0x0001
#define RINGFIRE_FLAG_POLICY_LOSSLESS_BACKPRESSURE 0x0002
#define RINGFIRE_FLAG_MODE_SPMC                 0x0010
#define RINGFIRE_FLAG_MODE_MPMC                 0x0020
#define RINGFIRE_FLAG_WITH_ARENA                0x0100
#define RINGFIRE_FLAG_WITH_REGISTRY             0x0200

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
    uint64_t schema_sig;          // Rust type layout fingerprint (0 = unverified)
    uint64_t arena_offset;        // Byte offset of the payload arena (0 = none)
    uint64_t arena_size;          // Payload arena capacity in bytes
    uint32_t reader_registry_offset; // Byte offset of the reader registry (0 = none)
    uint32_t reader_registry_count;  // Reader registry slots
    uint64_t slots_offset;        // Byte offset of slot 0 from the start of the mapping
    uint8_t _pad[16];             // Cache-line padding to 128 bytes
} ringfire_header_t;

#ifdef __cplusplus
static_assert(sizeof(ringfire_header_t) == 128, "ringfire_header_t must be 128 bytes");
#else
_Static_assert(sizeof(ringfire_header_t) == 128, "ringfire_header_t must be 128 bytes");
#endif

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
uint64_t ringfire_consumer_lapped_count(const ringfire_consumer_t* cons);
void ringfire_consumer_close(ringfire_consumer_t* cons);

ringfire_blackboard_t* ringfire_blackboard_create(const char* path, size_t slot_count, size_t value_size);
ringfire_blackboard_t* ringfire_blackboard_attach(const char* path, size_t value_size);
int ringfire_blackboard_write(ringfire_blackboard_t* bb, size_t key, const void* data);
/* Returns 1 = read, 0 = never written, -1 = bad arguments, -2 = writer stalled mid-update. */
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
    uint64_t capacity;
    uint64_t mask;
    uint64_t cursor;
    uint64_t lapped_count;
    uint32_t element_size;
    uint32_t slot_stride;
} ringfire_c_consumer_t;

static inline uint64_t ringfire_c_oldest_retained(uint64_t write_seq, uint64_t capacity) {
    return write_seq >= capacity ? write_seq - capacity + 1 : 1;
}

/**
 * @brief Attach to an existing ring buffer from pure C without linking any library.
 * @return 0 on success, -1 on I/O errors, -2 if not a v2 ring, -3 on record size mismatch,
 *         -4 if the header describes a layout that does not fit in the file.
 */
static inline int ringfire_c_consumer_attach(ringfire_c_consumer_t* cons, const char* path, uint32_t element_size) {
    cons->fd = -1;
    cons->mmap_ptr = NULL;

    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;

    struct stat st;
    if (fstat(fd, &st) < 0 || (size_t)st.st_size < sizeof(ringfire_header_t)) {
        close(fd);
        return -1;
    }

    void* ptr = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        close(fd);
        return -1;
    }

    ringfire_header_t* header = (ringfire_header_t*)ptr;
    uint64_t magic = *(volatile uint64_t*)&header->magic;
    atomic_thread_fence(memory_order_acquire);
    if (magic != RINGFIRE_MAGIC || header->version != RINGFIRE_VERSION) {
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

    uint64_t cap = header->capacity;
    if (cap == 0 || (cap & (cap - 1)) != 0 || header->mask != cap - 1 ||
        header->slots_offset < sizeof(ringfire_header_t) ||
        header->slots_offset + cap * slot_stride > (uint64_t)st.st_size) {
        munmap(ptr, (size_t)st.st_size);
        close(fd);
        return -4;
    }

    cons->fd = fd;
    cons->mmap_ptr = ptr;
    cons->total_size = (size_t)st.st_size;
    cons->header = header;
    cons->slots_base = (uint8_t*)ptr + header->slots_offset;
    cons->capacity = cap;
    cons->mask = header->mask;
    cons->element_size = element_size;
    cons->slot_stride = slot_stride;
    cons->lapped_count = 0;

    uint64_t write_seq = atomic_load_explicit(&header->write_seq, memory_order_acquire);
    cons->cursor = ringfire_c_oldest_retained(write_seq, cap);

    return 0;
}

/* Skips messages the producer overwrote. Returns 1 if the cursor moved. */
static inline int ringfire_c_consumer_skip_overwritten(ringfire_c_consumer_t* cons, uint64_t seen) {
    uint64_t write_seq = atomic_load_explicit(&cons->header->write_seq, memory_order_acquire);
    if (seen > write_seq) write_seq = seen;
    uint64_t oldest = ringfire_c_oldest_retained(write_seq, cons->capacity);
    if (oldest > cons->cursor) {
        cons->lapped_count += oldest - cons->cursor;
        cons->cursor = oldest;
        return 1;
    }
    return 0;
}

/**
 * @brief Zero-copy try_recv in pure C. Returns 1 if received, 0 if empty.
 *
 * A copy is accepted only if the slot held the wanted sequence before and after it was
 * taken. Messages lost to lapping are skipped and counted in `lapped_count`.
 */
static inline int ringfire_c_consumer_try_recv(ringfire_c_consumer_t* cons, void* out_data) {
    for (;;) {
        uint64_t want = cons->cursor;
        uint8_t* slot = cons->slots_base + ((want & cons->mask) * cons->slot_stride);
        _Atomic uint64_t* seq_ptr = (_Atomic uint64_t*)slot;
        uint64_t seen;

        uint64_t s1 = atomic_load_explicit(seq_ptr, memory_order_acquire);
        if (s1 == want) {
            memcpy(out_data, slot + 8, cons->element_size);
            atomic_thread_fence(memory_order_acquire);
            uint64_t s2 = atomic_load_explicit(seq_ptr, memory_order_relaxed);
            if (s2 == want) {
                cons->cursor++;
                return 1;
            }
            seen = (s2 == RINGFIRE_SLOT_WRITING) ? 0 : s2;
        } else if (s1 == RINGFIRE_SLOT_WRITING || s1 < want) {
            return 0; // Empty (or the slot is mid-write)
        } else {
            seen = s1; // Lapped by the producer
        }

        if (!ringfire_c_consumer_skip_overwritten(cons, seen)) {
            return 0;
        }
    }
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
