#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/eventfd.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

/*
 * A process wide io_uring used only to prefetch file ranges into the page cache (FADVISE / WILLNEED).
 * It is separate from the journal ring: prefetches are hints, they are fire and forget, and they must never
 * queue behind (or in front of) durable writes.
 *
 * Files are registered in the ring's fixed file table. The ring holds its own reference to the file, so a
 * request can never reach another file that reused the same descriptor number. Every slot has a generation:
 * a request carries the generation it was issued with, and the submitter drops it when the slot was released
 * since. A request that the kernel already holds keeps its file reference and completes harmlessly.
 *
 * Producers (any thread) push to a bounded lock-free queue; a full queue drops the request (it is only a hint).
 * One submitter thread drains the queue, submits in batches and reaps the completions.
 */

#define PREFETCH_RING_ENTRIES 256
#define PREFETCH_MAX_FILES 4096
#define PREFETCH_QUEUE_SIZE 8192 /* power of two */

struct prefetch_request
{
    _Atomic uint64_t sequence;
    int32_t slot;
    uint32_t generation;
    uint64_t offset;
    uint32_t length;
};

struct prefetch_state
{
    struct io_uring ring;
    int eventfd;
    pthread_t thread;
    _Atomic int32_t sleeping;
    _Atomic uint64_t enqueue_pos;
    _Atomic uint64_t dequeue_pos;
    struct prefetch_request queue[PREFETCH_QUEUE_SIZE];

    _Atomic uint32_t generations[PREFETCH_MAX_FILES];
    pthread_mutex_t slots_lock;
    int32_t free_slots[PREFETCH_MAX_FILES]; /* FIFO, so a released slot is reused as late as possible */
    int32_t free_head;
    int32_t free_count;

    _Atomic uint64_t submitted;
    _Atomic uint64_t dropped_full;
    _Atomic uint64_t dropped_stale;
    _Atomic uint64_t failed;
    int32_t inflight;
};

static struct prefetch_state *g_prefetch = NULL;
static pthread_once_t g_prefetch_once = PTHREAD_ONCE_INIT;
static int32_t g_prefetch_init_rc = SUCCESS;
static int32_t g_prefetch_init_errno = 0;

PRIVATE bool
_prefetch_dequeue(struct prefetch_state *s, struct prefetch_request *out)
{
    uint64_t pos = atomic_load_explicit(&s->dequeue_pos, memory_order_relaxed);
    for (;;)
    {
        struct prefetch_request *cell = &s->queue[pos & (PREFETCH_QUEUE_SIZE - 1)];
        uint64_t seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)(pos + 1);
        if (diff == 0)
        {
            if (atomic_compare_exchange_weak_explicit(&s->dequeue_pos, &pos, pos + 1, memory_order_relaxed, memory_order_relaxed))
            {
                out->slot = cell->slot;
                out->generation = cell->generation;
                out->offset = cell->offset;
                out->length = cell->length;
                atomic_store_explicit(&cell->sequence, pos + PREFETCH_QUEUE_SIZE, memory_order_release);
                return true;
            }
        }
        else if (diff < 0)
            return false; /* empty */
        else
            pos = atomic_load_explicit(&s->dequeue_pos, memory_order_relaxed);
    }
}

PRIVATE void
_prefetch_reap(struct prefetch_state *s, bool wait_for_one)
{
    struct io_uring_cqe *cqe;
    if (wait_for_one && s->inflight > 0)
    {
        if (io_uring_wait_cqe(&s->ring, &cqe) == 0)
        {
            if (cqe->res < 0)
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed);
            io_uring_cqe_seen(&s->ring, cqe);
            s->inflight--;
        }
    }
    while (s->inflight > 0 && io_uring_peek_cqe(&s->ring, &cqe) == 0)
    {
        if (cqe->res < 0)
            atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed);
        io_uring_cqe_seen(&s->ring, cqe);
        s->inflight--;
    }
}

PRIVATE void *
_prefetch_thread(void *arg)
{
    struct prefetch_state *s = arg;
    struct prefetch_request req;
    for (;;)
    {
        int prepared = 0;
        while (_prefetch_dequeue(s, &req))
        {
            if (atomic_load_explicit(&s->generations[req.slot], memory_order_acquire) != req.generation)
            {
                atomic_fetch_add_explicit(&s->dropped_stale, 1, memory_order_relaxed);
                continue;
            }
            if (s->inflight + prepared >= PREFETCH_RING_ENTRIES)
            {
                if (prepared > 0)
                {
                    io_uring_submit(&s->ring);
                    s->inflight += prepared;
                    prepared = 0;
                }
                _prefetch_reap(s, true);
            }
            struct io_uring_sqe *sqe = io_uring_get_sqe(&s->ring);
            if (sqe == NULL)
            {
                io_uring_submit(&s->ring);
                s->inflight += prepared;
                prepared = 0;
                _prefetch_reap(s, true);
                sqe = io_uring_get_sqe(&s->ring);
                if (sqe == NULL)
                {
                    atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
                    continue;
                }
            }
            io_uring_prep_fadvise(sqe, req.slot, req.offset, req.length, POSIX_FADV_WILLNEED);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, ((uint64_t)req.generation << 32) | (uint32_t)req.slot);
            prepared++;
            atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
        }
        if (prepared > 0)
        {
            io_uring_submit(&s->ring);
            s->inflight += prepared;
        }
        _prefetch_reap(s, false);

        /* sleep until a producer wakes us; re-check after announcing, so a push between the drain and the flag is not lost */
        atomic_store_explicit(&s->sleeping, 1, memory_order_seq_cst);
        uint64_t head = atomic_load_explicit(&s->dequeue_pos, memory_order_relaxed);
        struct prefetch_request *cell = &s->queue[head & (PREFETCH_QUEUE_SIZE - 1)];
        if ((int64_t)atomic_load_explicit(&cell->sequence, memory_order_acquire) - (int64_t)(head + 1) == 0)
        {
            atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
            continue;
        }
        uint64_t value;
        ssize_t r = read(s->eventfd, &value, sizeof(value));
        (void)r;
        atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
    }
    return NULL;
}

PRIVATE void
_prefetch_init_once(void)
{
    struct prefetch_state *s = calloc(1, sizeof(struct prefetch_state));
    if (s == NULL)
    {
        g_prefetch_init_rc = FAIL_NOMEM;
        return;
    }
    for (uint64_t i = 0; i < PREFETCH_QUEUE_SIZE; i++)
        atomic_store_explicit(&s->queue[i].sequence, i, memory_order_relaxed);
    for (int32_t i = 0; i < PREFETCH_MAX_FILES; i++)
        s->free_slots[i] = i;
    s->free_count = PREFETCH_MAX_FILES;
    pthread_mutex_init(&s->slots_lock, NULL);

    int rc = io_uring_queue_init(PREFETCH_RING_ENTRIES, &s->ring, 0);
    if (rc < 0)
    {
        g_prefetch_init_rc = FAIL_CREATE_IO_RING;
        g_prefetch_init_errno = -rc;
        free(s);
        return;
    }
    rc = io_uring_register_files_sparse(&s->ring, PREFETCH_MAX_FILES);
    if (rc < 0)
    {
        g_prefetch_init_rc = FAIL_CREATE_IO_RING;
        g_prefetch_init_errno = -rc;
        io_uring_queue_exit(&s->ring);
        free(s);
        return;
    }
    s->eventfd = eventfd(0, EFD_CLOEXEC);
    if (s->eventfd == -1 || pthread_create(&s->thread, NULL, _prefetch_thread, s) != 0)
    {
        g_prefetch_init_rc = FAIL_CREATE_IO_RING;
        g_prefetch_init_errno = errno;
        io_uring_queue_exit(&s->ring);
        free(s);
        return;
    }
    pthread_setname_np(s->thread, "Rvn.Prefetch");
    pthread_detach(s->thread);
    g_prefetch = s;
}

EXPORT int32_t
rvn_prefetch_register_file(int32_t fd, int32_t *slot, uint32_t *generation, int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    pthread_once(&g_prefetch_once, _prefetch_init_once);
    struct prefetch_state *s = g_prefetch;
    if (s == NULL)
    {
        *detailed_error_code = g_prefetch_init_errno;
        return g_prefetch_init_rc;
    }

    pthread_mutex_lock(&s->slots_lock);
    if (s->free_count == 0)
    {
        pthread_mutex_unlock(&s->slots_lock);
        *detailed_error_code = EMFILE;
        return FAIL;
    }
    int32_t chosen = s->free_slots[s->free_head];
    s->free_head = (s->free_head + 1) % PREFETCH_MAX_FILES;
    s->free_count--;
    pthread_mutex_unlock(&s->slots_lock);

    int rc = io_uring_register_files_update(&s->ring, (unsigned)chosen, &fd, 1);
    if (rc < 0)
    {
        pthread_mutex_lock(&s->slots_lock);
        s->free_slots[(s->free_head + s->free_count) % PREFETCH_MAX_FILES] = chosen;
        s->free_count++;
        pthread_mutex_unlock(&s->slots_lock);
        *detailed_error_code = -rc;
        return FAIL;
    }
    *slot = chosen;
    *generation = atomic_load_explicit(&s->generations[chosen], memory_order_acquire);
    return SUCCESS;
}

EXPORT int32_t
rvn_prefetch_unregister_file(int32_t slot, int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    struct prefetch_state *s = g_prefetch;
    if (s == NULL || slot < 0 || slot >= PREFETCH_MAX_FILES)
        return FAIL_INVALID_HANDLE;

    /* queued requests for the old generation are dropped by the submitter; in flight ones hold their own file reference */
    atomic_fetch_add_explicit(&s->generations[slot], 1, memory_order_acq_rel);
    int none = -1;
    int rc = io_uring_register_files_update(&s->ring, (unsigned)slot, &none, 1);

    pthread_mutex_lock(&s->slots_lock);
    s->free_slots[(s->free_head + s->free_count) % PREFETCH_MAX_FILES] = slot;
    s->free_count++;
    pthread_mutex_unlock(&s->slots_lock);

    if (rc < 0)
    {
        *detailed_error_code = -rc;
        return FAIL;
    }
    return SUCCESS;
}

/* returns 1 when queued, 0 when dropped (queue full or ring not available) */
EXPORT int32_t
rvn_prefetch_file_range(int32_t slot, uint32_t generation, int64_t offset, int64_t length)
{
    struct prefetch_state *s = g_prefetch;
    if (s == NULL || slot < 0 || slot >= PREFETCH_MAX_FILES || length <= 0)
        return 0;
    if (length > UINT32_MAX)
        length = UINT32_MAX;

    uint64_t pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    struct prefetch_request *cell;
    for (;;)
    {
        cell = &s->queue[pos & (PREFETCH_QUEUE_SIZE - 1)];
        uint64_t seq = atomic_load_explicit(&cell->sequence, memory_order_acquire);
        int64_t diff = (int64_t)seq - (int64_t)pos;
        if (diff == 0)
        {
            if (atomic_compare_exchange_weak_explicit(&s->enqueue_pos, &pos, pos + 1, memory_order_relaxed, memory_order_relaxed))
                break;
        }
        else if (diff < 0)
        {
            atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
            return 0;
        }
        else
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    }
    cell->slot = slot;
    cell->generation = generation;
    cell->offset = (uint64_t)offset;
    cell->length = (uint32_t)length;
    atomic_store_explicit(&cell->sequence, pos + 1, memory_order_release);

    if (atomic_load_explicit(&s->sleeping, memory_order_seq_cst))
    {
        uint64_t one = 1;
        ssize_t r = write(s->eventfd, &one, sizeof(one));
        (void)r;
    }
    return 1;
}

EXPORT int32_t
rvn_prefetch_stats(int64_t *submitted, int64_t *dropped_full, int64_t *dropped_stale, int64_t *failed)
{
    struct prefetch_state *s = g_prefetch;
    if (s == NULL)
    {
        *submitted = *dropped_full = *dropped_stale = *failed = 0;
        return FAIL_INVALID_HANDLE;
    }
    *submitted = (int64_t)atomic_load(&s->submitted);
    *dropped_full = (int64_t)atomic_load(&s->dropped_full);
    *dropped_stale = (int64_t)atomic_load(&s->dropped_stale);
    *failed = (int64_t)atomic_load(&s->failed);
    return SUCCESS;
}

/* synchronous variant for callers that are about to touch the range themselves (one exact read instead of two faults) */
EXPORT int32_t
rvn_fadvise_willneed(int32_t fd, int64_t offset, int64_t length)
{
    return posix_fadvise(fd, offset, length, POSIX_FADV_WILLNEED);
}
