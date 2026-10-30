#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <sys/uio.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

/*
 * An io_uring for reads into the page cache, separate from the journal ring so it never queues behind or in front of
 * durable writes. Any thread queues requests; one submitter thread runs them. Two kinds of requests:
 *  - WILLNEED range: a hint, dropped when the queue is full.
 *  - read group: an 8-byte read across the 4 KB boundary inside each 8 KB page, so each page comes in one I/O. The token
 *    reaches on_reads_done once all of the group's reads complete, and the caller keeps the offsets until then.
 *
 * A registered file is in the ring's fixed file table, and the ring holds its own reference to it, so a request never
 * reaches another file that reused the descriptor number. A ticket is (generation << 32) | slot; registering and
 * unregistering bump the slot's generation, so the submitter drops queued requests of an earlier registration.
 *
 * Without io_uring, the submitter runs WILLNEED with posix_fadvise on a duplicate descriptor, and read groups complete at
 * once without reads. A request that races an unregistration may then reach a reused descriptor number: a wasted
 * read-ahead, never a write.
 */

#define CACHE_LINE 64
#define PREFETCH_READ_AT (4096 - 4) /* 4 bytes on each side of the 4 KB boundary inside a page */
#define PREFETCH_READ_LENGTH 8
#define PREFETCH_PAGE_SIZE 8192
#define PREFETCH_GROUP_TAG (1ULL << 63) /* user_data of a group read: the tag | the group index; WILLNEED uses 0 */
#define PREFETCH_DONE_BATCH 64

#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif

struct prefetch_request
{
    int64_t ticket;
    uint64_t offset;          /* WILLNEED range */
    uint32_t length;
    uint32_t count;           /* read group: the number of offsets; 0 for WILLNEED */
    uint64_t token;
    const int64_t *offsets;
};

struct prefetch_cell
{
    /* Vyukov bounded queue: a cell at position pos is free while sequence == pos, and holds a request
       while sequence == pos + 1; emptying it sets sequence = pos + queue size (free for the next lap) */
    _Atomic uint64_t sequence;
    struct prefetch_request request;
};

struct read_group
{
    uint64_t token;
    uint32_t pending;
    int32_t next_free;
};

#define NO_FREE_GROUP (-1)

struct prefetch_state
{
    /* written by every producer */
    alignas(CACHE_LINE) _Atomic uint64_t enqueue_pos;
    _Atomic uint64_t dropped_full;
    /* written by the submitter, read by every producer */
    alignas(CACHE_LINE) _Atomic int32_t sleeping;
    _Atomic int32_t stopping;
    /* owned by the submitter thread (the only consumer); the counters are read by rvn_prefetch_ring_stats */
    alignas(CACHE_LINE) uint64_t dequeue_pos;
    uint32_t inflight;
    _Atomic uint64_t submitted;
    _Atomic uint64_t dropped_stale;
    _Atomic uint64_t failed;
    _Atomic uint64_t read_groups;
    _Atomic uint64_t reads;

    struct read_group *groups;
    int32_t free_group;
    uint64_t done[PREFETCH_DONE_BATCH]; /* completed tokens not yet passed to on_reads_done */
    int32_t done_count;
    alignas(CACHE_LINE) char sink[CACHE_LINE]; /* registered buffer 0: the target of every read, never read */

    /* set once at init */
    alignas(CACHE_LINE) struct io_uring ring;
    bool use_io_uring;
    int *fds; /* without io_uring: a duplicate of each registered file's descriptor, -1 when free */
    int eventfd;
    pthread_t thread;
    uint32_t ring_entries;
    uint64_t queue_mask;
    int32_t max_files;
    int32_t max_groups;
    rvn_reads_done on_reads_done;
    void *on_reads_done_context;

    /* registration, guarded by slots_lock */
    pthread_mutex_t slots_lock;
    int32_t free_head;
    int32_t free_count;
    int32_t *free_slots; /* FIFO ring of free slot numbers, so a released slot is reused as late as possible */
    _Atomic uint32_t *generations;

    struct prefetch_cell *queue;
};

static inline int32_t _ticket_slot(int64_t ticket) { return (int32_t)(uint32_t)ticket; }
static inline uint32_t _ticket_generation(int64_t ticket) { return (uint32_t)((uint64_t)ticket >> 32); }
static inline int64_t _make_ticket(int32_t slot, uint32_t generation) { return (int64_t)(((uint64_t)generation << 32) | (uint32_t)slot); }

PRIVATE bool
_prefetch_dequeue(struct prefetch_state *s, struct prefetch_request *out)
{
    struct prefetch_cell *cell = &s->queue[s->dequeue_pos & s->queue_mask];
    if (atomic_load_explicit(&cell->sequence, memory_order_acquire) != s->dequeue_pos + 1)
        return false; /* empty, or the producer has not published the cell yet */
    *out = cell->request;
    atomic_store_explicit(&cell->sequence, s->dequeue_pos + s->queue_mask + 1, memory_order_release);
    s->dequeue_pos++;
    return true;
}

PRIVATE void
_flush_done(struct prefetch_state *s)
{
    if (s->done_count == 0)
        return;
    s->on_reads_done(s->on_reads_done_context, s->done, s->done_count);
    s->done_count = 0;
}

PRIVATE void
_group_done(struct prefetch_state *s, uint64_t token)
{
    if (s->done_count == PREFETCH_DONE_BATCH)
        _flush_done(s);
    s->done[s->done_count++] = token;
}

PRIVATE void
_group_read_done(struct prefetch_state *s, int32_t group)
{
    struct read_group *g = &s->groups[group];
    if (--g->pending != 0)
        return;
    g->next_free = s->free_group;
    s->free_group = group;
    _group_done(s, g->token);
}

PRIVATE void
_prefetch_complete(struct prefetch_state *s, struct io_uring_cqe *cqe)
{
    if (cqe->res < 0)
        atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* a failed read still completes its group */
    uint64_t data = io_uring_cqe_get_data64(cqe);
    io_uring_cqe_seen(&s->ring, cqe);
    s->inflight--;
    if (data & PREFETCH_GROUP_TAG)
        _group_read_done(s, (int32_t)(data & ~PREFETCH_GROUP_TAG));
}

PRIVATE void
_prefetch_reap(struct prefetch_state *s, bool wait_for_one)
{
    struct io_uring_cqe *cqe;
    if (wait_for_one)
        _flush_done(s); /* completed groups must not wait while we block */
    if (wait_for_one && s->inflight > 0 && io_uring_wait_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
    while (s->inflight > 0 && io_uring_peek_cqe(&s->ring, &cqe) == 0)
        _prefetch_complete(s, cqe);
}

PRIVATE void
_prefetch_submit(struct prefetch_state *s)
{
    int rc = io_uring_submit(&s->ring);
    if (rc > 0)
        s->inflight += (uint32_t)rc; /* only what the kernel took; the rest stays in the SQ for the next submit */
}

/* In flight + prepared never exceed the SQ size, so the CQ (twice that) cannot overflow: when full, submit and wait for
   one completion. */
PRIVATE struct io_uring_sqe *
_prefetch_get_sqe(struct prefetch_state *s)
{
    if (s->inflight + io_uring_sq_ready(&s->ring) >= s->ring_entries)
    {
        _prefetch_submit(s);
        _prefetch_reap(s, true);
    }
    return io_uring_get_sqe(&s->ring);
}

PRIVATE void
_prefetch_read_group(struct prefetch_state *s, int32_t slot, const struct prefetch_request *req)
{
    atomic_fetch_add_explicit(&s->read_groups, 1, memory_order_relaxed);
    if (s->use_io_uring == false || s->free_group == NO_FREE_GROUP)
    {
        _group_done(s, req->token);
        return;
    }
    int32_t group = s->free_group;
    struct read_group *g = &s->groups[group];
    s->free_group = g->next_free;
    g->token = req->token;
    g->pending = req->count;
    for (uint32_t i = 0; i < req->count; i++)
    {
        struct io_uring_sqe *sqe = _prefetch_get_sqe(s); /* may reap: this group's earlier reads can complete here */
        if (sqe == NULL)
        {
            atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the submit failed and the SQ is still full */
            _group_read_done(s, group);
            continue;
        }
        io_uring_prep_read_fixed(sqe, slot, s->sink, PREFETCH_READ_LENGTH, (uint64_t)req->offsets[i] + PREFETCH_READ_AT, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        io_uring_sqe_set_data64(sqe, PREFETCH_GROUP_TAG | (uint64_t)group);
        atomic_fetch_add_explicit(&s->reads, 1, memory_order_relaxed);
    }
}

PRIVATE void *
rvn_prefetch_ring_submitter(void *arg)
{
    struct prefetch_state *s = arg;
    struct prefetch_request req;
    while (atomic_load_explicit(&s->stopping, memory_order_acquire) == 0)
    {
        while (_prefetch_dequeue(s, &req))
        {
            int32_t slot = _ticket_slot(req.ticket);
            if (atomic_load_explicit(&s->generations[slot], memory_order_acquire) != _ticket_generation(req.ticket))
            {
                atomic_fetch_add_explicit(&s->dropped_stale, 1, memory_order_relaxed);
                if (req.count != 0)
                    _group_done(s, req.token); /* the file was closed: nothing to wait for */
                continue;
            }
            if (req.count != 0)
            {
                _prefetch_read_group(s, slot, &req);
                continue;
            }
            if (s->use_io_uring == false)
            {
                int fd = s->fds[slot];
                if (fd == -1 || posix_fadvise(fd, (off_t)req.offset, (off_t)req.length, POSIX_FADV_WILLNEED) != 0)
                    atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed);
                else
                    atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
                continue;
            }
            struct io_uring_sqe *sqe = _prefetch_get_sqe(s);
            if (sqe == NULL)
            {
                atomic_fetch_add_explicit(&s->failed, 1, memory_order_relaxed); /* the submit failed and the SQ is still full */
                continue;
            }
            io_uring_prep_fadvise(sqe, slot, req.offset, req.length, POSIX_FADV_WILLNEED);
            sqe->flags |= IOSQE_FIXED_FILE;
            io_uring_sqe_set_data64(sqe, 0);
            atomic_fetch_add_explicit(&s->submitted, 1, memory_order_relaxed);
        }
        if (s->use_io_uring)
        {
            if (io_uring_sq_ready(&s->ring) > 0)
                _prefetch_submit(s);
            _prefetch_reap(s, false);
        }
        _flush_done(s);

        /* sleep until a producer or a completion signals the eventfd; re-check after announcing, so a push between the drain
           and the flag is not lost */
        atomic_store_explicit(&s->sleeping, 1, memory_order_seq_cst);
        struct prefetch_cell *cell = &s->queue[s->dequeue_pos & s->queue_mask];
        if (atomic_load_explicit(&cell->sequence, memory_order_seq_cst) == s->dequeue_pos + 1)
        {
            atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
            continue;
        }
        if (atomic_load_explicit(&s->stopping, memory_order_acquire))
            break;
        uint64_t value;
        ssize_t r = read(s->eventfd, &value, sizeof(value));
        (void)r;
        atomic_store_explicit(&s->sleeping, 0, memory_order_relaxed);
    }
    while (_prefetch_dequeue(s, &req)) /* stopping: a group still in the queue completes without its reads */
    {
        if (req.count != 0)
            _group_done(s, req.token);
    }
    while (s->inflight > 0) /* in-flight reads write into the sink, which is freed with the state */
        _prefetch_reap(s, true);
    _flush_done(s);
    return NULL;
}

PRIVATE void
_prefetch_free(struct prefetch_state *s)
{
    free(s->fds);
    free(s->groups);
    free(s->queue);
    free(s->free_slots);
    free((void *)s->generations);
    free(s);
}

EXPORT int32_t
rvn_prefetch_ring_create(int32_t ring_entries, int32_t max_files, int32_t queue_size,
    rvn_reads_done on_reads_done, void *on_reads_done_context, void **ring, int32_t *detailed_error_code)
{
    *ring = NULL;
    *detailed_error_code = 0;
    if (ring_entries <= 0 || max_files <= 0 || queue_size <= 0 || (queue_size & (queue_size - 1)) != 0 || on_reads_done == NULL)
        return FAIL_INVALID_CONFIGURATION; /* the queue indexes by mask: a power of two */

    struct prefetch_state *s = NULL;
    size_t state_size = (sizeof(struct prefetch_state) + CACHE_LINE - 1) & ~(size_t)(CACHE_LINE - 1);
    if (posix_memalign((void **)&s, CACHE_LINE, state_size) != 0)
    {
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    memset(s, 0, state_size);
    s->eventfd = -1;
    s->ring_entries = (uint32_t)ring_entries;
    s->max_files = max_files;
    s->on_reads_done = on_reads_done;
    s->on_reads_done_context = on_reads_done_context;
    s->max_groups = ring_entries + 1; /* each group in flight holds a ring entry, except the one being prepared */
    uint64_t size = (uint64_t)queue_size;
    s->queue_mask = size - 1;

    s->queue = calloc(size, sizeof(struct prefetch_cell));
    s->free_slots = calloc((size_t)max_files, sizeof(int32_t));
    s->generations = calloc((size_t)max_files, sizeof(_Atomic uint32_t));
    s->groups = calloc((size_t)s->max_groups, sizeof(struct read_group));
    if (s->queue == NULL || s->free_slots == NULL || s->generations == NULL || s->groups == NULL)
    {
        _prefetch_free(s);
        *detailed_error_code = ENOMEM;
        return FAIL_NOMEM;
    }
    for (uint64_t i = 0; i < size; i++)
        atomic_store_explicit(&s->queue[i].sequence, i, memory_order_relaxed); /* every cell free for the first lap */
    for (int32_t i = 0; i < max_files; i++)
        s->free_slots[i] = i;
    s->free_count = max_files;
    for (int32_t i = 0; i < s->max_groups; i++)
        s->groups[i].next_free = i + 1 < s->max_groups ? i + 1 : NO_FREE_GROUP;
    s->free_group = 0;
    pthread_mutex_init(&s->slots_lock, NULL);

    s->eventfd = eventfd(0, EFD_CLOEXEC);
    if (s->eventfd == -1)
    {
        *detailed_error_code = errno;
        _prefetch_free(s);
        return FAIL_CREATE_THREAD;
    }
    s->use_io_uring = io_uring_queue_init(s->ring_entries, &s->ring, 0) == 0;
    struct iovec sink = { .iov_base = s->sink, .iov_len = sizeof(s->sink) };
    if (s->use_io_uring &&
        (io_uring_register_files_sparse(&s->ring, (unsigned)max_files) < 0 || io_uring_register_eventfd(&s->ring, s->eventfd) < 0 ||
         io_uring_register_buffers(&s->ring, &sink, 1) < 0))
    {
        io_uring_queue_exit(&s->ring);
        s->use_io_uring = false;
    }
    if (s->use_io_uring == false)
    {
        s->fds = malloc((size_t)max_files * sizeof(int));
        if (s->fds == NULL)
        {
            close(s->eventfd);
            _prefetch_free(s);
            *detailed_error_code = ENOMEM;
            return FAIL_NOMEM;
        }
        for (int32_t i = 0; i < max_files; i++)
            s->fds[i] = -1;
    }
    int rc = pthread_create(&s->thread, NULL, rvn_prefetch_ring_submitter, s); /* returns the error, errno is not set */
    if (rc != 0)
    {
        *detailed_error_code = rc;
        close(s->eventfd);
        if (s->use_io_uring)
            io_uring_queue_exit(&s->ring);
        _prefetch_free(s);
        return FAIL_CREATE_THREAD;
    }
    pthread_setname_np(s->thread, "Rvn.Prefetch");
    *ring = s;
    return SUCCESS;
}

/* Stops the submitter and frees the ring. Fails while files are still registered with it. */
EXPORT int32_t
rvn_prefetch_ring_destroy(void *ring, int32_t *detailed_error_code)
{
    *detailed_error_code = 0;
    struct prefetch_state *s = ring;
    if (s == NULL)
        return FAIL_INVALID_HANDLE;

    pthread_mutex_lock(&s->slots_lock);
    int32_t registered = s->max_files - s->free_count;
    pthread_mutex_unlock(&s->slots_lock);
    if (registered != 0)
    {
        *detailed_error_code = EBUSY;
        return FAIL_INVALID_HANDLE;
    }

    atomic_store_explicit(&s->stopping, 1, memory_order_seq_cst);
    uint64_t one = 1;
    ssize_t r = write(s->eventfd, &one, sizeof(one));
    (void)r;
    pthread_join(s->thread, NULL);

    if (s->use_io_uring)
        io_uring_queue_exit(&s->ring);
    close(s->eventfd);
    pthread_mutex_destroy(&s->slots_lock);
    _prefetch_free(s);
    return SUCCESS;
}

/* Registers fd with the ring. A full file table is an error. */
PRIVATE int32_t
rvn_prefetch_register(void *ring, int fd, int64_t *ticket, int32_t *detailed_error_code)
{
    *ticket = -1;
    struct prefetch_state *s = ring;
    if (s == NULL)
        return FAIL_INVALID_HANDLE;

    pthread_mutex_lock(&s->slots_lock);
    if (s->free_count == 0)
    {
        pthread_mutex_unlock(&s->slots_lock);
        *detailed_error_code = EMFILE;
        return FAIL_PREFETCH_FILES_EXHAUSTED;
    }
    int32_t slot = s->free_slots[s->free_head];
    if (s->use_io_uring)
    {
        int rc = io_uring_register_files_update(&s->ring, (unsigned)slot, &fd, 1);
        if (rc < 0)
        {
            pthread_mutex_unlock(&s->slots_lock);
            *detailed_error_code = -rc;
            return FAIL_CREATE_IO_RING;
        }
    }
    else
    {
        int dup_fd = dup(fd); /* the pager may close and reopen its descriptor; the duplicate lives until unregister */
        if (dup_fd == -1)
        {
            *detailed_error_code = errno;
            pthread_mutex_unlock(&s->slots_lock);
            return FAIL_DUPLICATE_HANDLE;
        }
        s->fds[slot] = dup_fd;
    }
    s->free_head = (s->free_head + 1) % s->max_files;
    s->free_count--;
    uint32_t generation = atomic_fetch_add_explicit(&s->generations[slot], 1, memory_order_acq_rel) + 1;
    pthread_mutex_unlock(&s->slots_lock);

    *ticket = _make_ticket(slot, generation);
    return SUCCESS;
}

PRIVATE void
rvn_prefetch_unregister(void *ring, int64_t ticket)
{
    struct prefetch_state *s = ring;
    int32_t slot = _ticket_slot(ticket);
    if (s == NULL || ticket == -1 || slot < 0 || slot >= s->max_files)
        return;

    pthread_mutex_lock(&s->slots_lock);
    /* queued requests carry the old generation and are dropped; in flight ones hold their own file reference */
    atomic_fetch_add_explicit(&s->generations[slot], 1, memory_order_acq_rel);
    if (s->use_io_uring)
    {
        int none = -1;
        (void)io_uring_register_files_update(&s->ring, (unsigned)slot, &none, 1);
    }
    else
    {
        close(s->fds[slot]);
        s->fds[slot] = -1;
    }
    s->free_slots[(s->free_head + s->free_count) % s->max_files] = slot;
    s->free_count++;
    pthread_mutex_unlock(&s->slots_lock);
}

/* returns 1 when queued, 0 when dropped (the queue is full) */
PRIVATE int32_t
_prefetch_push(struct prefetch_state *s, const struct prefetch_request *req)
{
    /* pos must outlive the loop: a failed CAS reloads it, and the publish below needs it */
    uint64_t pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
    struct prefetch_cell *cell;
    for (;;)
    {
        cell = &s->queue[pos & s->queue_mask];
        int64_t diff = (int64_t)atomic_load_explicit(&cell->sequence, memory_order_acquire) - (int64_t)pos;
        if (diff == 0)
        {
            if (atomic_compare_exchange_weak_explicit(&s->enqueue_pos, &pos, pos + 1, memory_order_relaxed, memory_order_relaxed))
                break;
        }
        else if (diff < 0)
        {
            /* the cell still holds a request from the previous lap: the queue is full */
            atomic_fetch_add_explicit(&s->dropped_full, 1, memory_order_relaxed);
            return 0;
        }
        else
        {
            pos = atomic_load_explicit(&s->enqueue_pos, memory_order_relaxed);
        }
    }
    cell->request = *req;
    /* seq_cst, not release: with the submitter's seq_cst store of sleeping and its re-check this is a Dekker pair,
       so either we see sleeping == 1 and wake it, or it sees this cell and does not sleep */
    atomic_store_explicit(&cell->sequence, pos + 1, memory_order_seq_cst);

    if (atomic_load_explicit(&s->sleeping, memory_order_seq_cst))
    {
        uint64_t one = 1;
        ssize_t r = write(s->eventfd, &one, sizeof(one));
        (void)r;
    }
    return 1;
}

PRIVATE bool
_prefetch_valid_ticket(struct prefetch_state *s, int64_t ticket)
{
    int32_t slot = _ticket_slot(ticket);
    return s != NULL && ticket != -1 && slot >= 0 && slot < s->max_files;
}

/* returns 1 when queued, 0 when dropped (queue full or file not registered) */
PRIVATE int32_t
rvn_prefetch_enqueue(void *ring, int64_t ticket, int64_t offset, int64_t length)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || length <= 0)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    req.offset = (uint64_t)offset;
    req.length = length > UINT32_MAX ? UINT32_MAX : (uint32_t)length;
    return _prefetch_push(s, &req);
}

/* Returns 1 when queued: the token reaches on_reads_done once, and offsets must stay valid until then. Returns 0 when not
   queued (queue full, file not registered, no pages): the token never reaches on_reads_done. */
PRIVATE int32_t
rvn_prefetch_enqueue_reads(void *ring, int64_t ticket, const int64_t *offsets, int32_t count, uint64_t token)
{
    struct prefetch_state *s = ring;
    if (_prefetch_valid_ticket(s, ticket) == false || offsets == NULL || count <= 0)
        return 0;
    struct prefetch_request req = {0};
    req.ticket = ticket;
    req.count = (uint32_t)count;
    req.token = token;
    req.offsets = offsets;
    return _prefetch_push(s, &req);
}

PRIVATE ssize_t
_pread_nowait(int fd, void *buffer, size_t length, int64_t offset)
{
    struct iovec iov = { .iov_base = buffer, .iov_len = length };
#if __SIZEOF_LONG__ == 8
    return syscall(SYS_preadv2, fd, &iov, 1, (long)offset, 0L, RWF_NOWAIT);
#else
    return syscall(SYS_preadv2, fd, &iov, 1, (long)(uint32_t)offset, (long)((uint64_t)offset >> 32), RWF_NOWAIT);
#endif
}

/* A non-blocking 8-byte read of each 8 KB page: it returns at once when the page is in the page cache, and otherwise starts
   reading it (the file has FADV_RANDOM, so exactly that page). Moves the offsets of the pages that were not in the page cache
   to the front and returns their number. */
PRIVATE int32_t
rvn_prefetch_start_reads(int fd, int64_t *offsets, int32_t count)
{
    char sink[PREFETCH_READ_LENGTH];
    int32_t missing = 0;
    for (int32_t i = 0; i < count; i++)
    {
        int64_t offset = offsets[i];
        ssize_t rc = _pread_nowait(fd, sink, sizeof(sink), offset + PREFETCH_READ_AT);
        if (rc == PREFETCH_READ_LENGTH)
            continue;
        if (rc == -1 && (errno == EOPNOTSUPP || errno == ENOSYS)) /* the file system or the kernel has no RWF_NOWAIT */
            (void)posix_fadvise(fd, (off_t)offset, PREFETCH_PAGE_SIZE, POSIX_FADV_WILLNEED);
        offsets[missing++] = offset;
    }
    return missing;
}

EXPORT int32_t
rvn_prefetch_ring_stats(void *ring, int64_t *submitted, int64_t *dropped_full, int64_t *dropped_stale, int64_t *failed,
    int64_t *read_groups, int64_t *reads, int32_t *uses_io_uring)
{
    struct prefetch_state *s = ring;
    if (s == NULL)
    {
        *submitted = *dropped_full = *dropped_stale = *failed = *read_groups = *reads = 0;
        *uses_io_uring = 0;
        return FAIL_INVALID_HANDLE;
    }
    *submitted = (int64_t)atomic_load(&s->submitted);
    *dropped_full = (int64_t)atomic_load(&s->dropped_full);
    *dropped_stale = (int64_t)atomic_load(&s->dropped_stale);
    *failed = (int64_t)atomic_load(&s->failed);
    *read_groups = (int64_t)atomic_load(&s->read_groups);
    *reads = (int64_t)atomic_load(&s->reads);
    *uses_io_uring = s->use_io_uring ? 1 : 0;
    return SUCCESS;
}
