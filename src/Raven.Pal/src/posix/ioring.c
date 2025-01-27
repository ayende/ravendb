#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/utsname.h>
#include <unistd.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <linux/ioprio.h>
#include <sys/syscall.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/eventfd.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

typedef enum workitem_type
{
    workitem_none,
    workitem_write,
    workitem_fsync,
} workitem_type;

struct workitem
{
    struct workitem *next;
    struct workitem *prev;
    off_t offset;
    _Atomic int completed;

    int filefd;
    int notifyfd;
    int result;
    workitem_type type;
    bool errored;
    int iovec_count;
    struct iovec *iovecs;
};

_Static_assert(_Alignof(struct workitem) == _Alignof(struct iovec), "workitem must have same alignment as iovec");

struct worker
{
    struct io_uring ring;
    int eventfd;
    _Atomic bool errored;
    int result;
    pthread_t thread;
    struct workitem *_Atomic head;
};

struct worker g_worker = {.eventfd = -1, .ring = {.ring_fd = -1}};

uint64_t done = 1;

void notify_work_completed(struct io_uring *ring, struct workitem *work)
{
    atomic_store(&work->completed, 1);
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (!sqe) // if we can't notify via the io_uring because it is full, let's use direct syscall
    {
        // explicitly ignoring the return value, we can't do anything about it
        eventfd_write(work->notifyfd, done);
        return;
    }
    io_uring_prep_write(sqe, work->notifyfd, &done, sizeof(done), 0);
    io_uring_sqe_set_data(sqe, 0);
}

void queue_work(struct workitem *work)
{
    struct workitem *head = atomic_load(&g_worker.head);
    do
    {
        work->next = head;
    } while (!atomic_compare_exchange_weak(&g_worker.head, &head, work));
}

void mark_all_cqes_as_errors(struct io_uring *ring, int rc)
{
    struct io_uring_cqe *cqe;
    struct __kernel_timespec timeout = {.tv_sec = 0, .tv_nsec = 100 * 1000000}; // 100ms
    while (!io_uring_wait_cqe_timeout(ring, &cqe, &timeout))
    {
        struct workitem *work = io_uring_cqe_get_data(cqe);
        io_uring_cqe_seen(ring, cqe);
        work->result = rc;
        work->errored = true;
        eventfd_write(work->notifyfd, 1);
    }
}

void close_ring_with_error(struct io_uring *ring, int rc)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe)
    {
        io_uring_prep_cancel(sqe, 0, 0); // Cancel all, if possible
        io_uring_submit_and_get_events(ring);
    }
    mark_all_cqes_as_errors(ring, rc);
    io_uring_queue_exit(ring);
}

void *do_ring_work(void *arg)
{
    // Set I/O priority to a low value (Best Effort class with lowest priority within class)
    // since we want to use this thread for running flush & sync operation in the background
    // and those are not urgent. We'll let the OS schedule them as needed.
    int rc;
    if (g_cfg.low_priority_io)
    {
        int ioprio = IOPRIO_PRIO_VALUE(IOPRIO_CLASS_BE, 7);                  // 7 is the lowest priority in Best Effort class
        int result = syscall(SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, ioprio); // 0 means current thread
        (void)result;                                                        // explicitly ignoring this error, it doesn't matter if we cannot do that
    }
    pthread_setname_np(pthread_self(), "Rvn.Ring.Wrkr");

    struct pollfd pfd = {.fd = g_worker.eventfd, .events = POLLIN};

    struct io_uring *ring = &g_worker.ring;
    struct workitem *work = NULL;
    while (true)
    {
        // wait for any writes on the eventfd / completion on the ring (associated with the eventfd)
        if (poll(&pfd, 1, -1) == -1)
        {
            if (errno == EINTR)
                continue;
            rc = errno;
            goto error;
        }
        bool has_work = true;
        bool must_wait = false;
        while (has_work)
        {
            has_work = false;
            if (!work) // we may have _previous_ work to run through
            {
                work = atomic_exchange(&g_worker.head, 0);
            }
            while (work)
            {
                has_work = true;
                struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
                if (sqe == NULL)
                {
                    must_wait = true;
                    break; // need to submit the work we have so far, and retry later
                }
                io_uring_sqe_set_data(sqe, work);
                switch (work->type)
                {
                case workitem_fsync:
                    io_uring_prep_fsync(sqe, work->filefd, IORING_FSYNC_DATASYNC);
                    break;
                case workitem_write:
                    io_uring_prep_writev(sqe, work->filefd, work->iovecs, work->iovec_count, work->offset);
                    break;
                default:
                    break;
                }

                work = work->next;
            }
            rc = must_wait ? io_uring_submit_and_wait(ring, 1) : io_uring_submit(ring);
            if (rc < 0)
            {
                rc = -rc;
                goto error;
            }
            struct io_uring_cqe *cqe;
            uint32_t head = 0;
            uint32_t i = 0;

            io_uring_for_each_cqe(ring, head, cqe)
            {
                i++;
                has_work = true; // force another run of the inner loop, to ensure that we call io_uring_submit again
                struct workitem *cur = io_uring_cqe_get_data(cqe);

                if (!cur)
                {
                    continue; // can be null if it is a notification about eventfd write
                }

                int result = cqe->res;
                cur->result = -result;
                switch (cur->type)
                {
                case workitem_fsync:
                    cur->errored = result != 0;
                    notify_work_completed(ring, cur);
                    break;
                case workitem_write:
                    if (result > 0)
                    {
                        cur->offset += result;
                        while (result)
                        {
                            if (result >= cur->iovecs->iov_len)
                            {
                                result -= cur->iovecs->iov_len;
                                cur->iovecs++;
                                cur->iovec_count--;
                            }
                            else
                            {
                                cur->iovecs->iov_len -= result;
                                cur->iovecs->iov_base += result;
                                break;
                            }
                        }
                        if (result)
                        {
                            queue_work(cur);
                            continue;
                        }
                    }
                    else
                    {
                        cur->errored = true;
                        if (result == 0)
                        {
                            // this usually happens if we a disk full, or some
                            cur->result = ENOSPC;
                        }
                    }
                    notify_work_completed(ring, cur);
                    break;
                default:
                    rc = FAIL_INVALID_CONFIGURATION;
                    goto error;
                }
            }
            io_uring_cq_advance(ring, i);
        }
    }

error:
    atomic_store(&g_worker.errored, rc);
    close(g_worker.eventfd);
    g_worker.eventfd = -1;
    close_ring_with_error(ring, rc);
    g_worker.ring = (struct io_uring){.ring_fd = -1};
    while (true)
    {
        if (!work) // fill from the global list if needed
        {
            work = atomic_exchange(&g_worker.head, 0);
            if (!work)
                break; // nothing remains to be done
        }
        while (work)
        {
            work->result = rc;
            work->errored = true;
            eventfd_write(work->notifyfd, 1);
            work = work->next;
        }
    }
    return 0;
}

bool _io_ring_supported()
{
    if (sizeof(void *) != 8)
        return 0; // not supported on 32 bits

    struct utsname buffer;
    if (uname(&buffer) != 0)
        return 0;

    int curr_major = 0, curr_minor = 0, curr_patch = 0;
    sscanf(buffer.release, "%d.%d.%d", &curr_major, &curr_minor, &curr_patch);

    // we require at least 5.10, because we need IOSQE_FIXED_FILE | IOSQE_IO_LINK & IORING_FEAT_NODROP
    return curr_major > 5 || (curr_major == 5 && curr_minor >= 10);
}

bool wake_ring_worker(int32_t *detailed_error_code)
{
    if (eventfd_write(g_worker.eventfd, 1))
    {
        *detailed_error_code = errno;
        // this means that the ring is probably dead, which is a catastrophic error
        // need to wait for the relevant thread to complete, to ensure we aren't
        // using the values we submitted to the ring
        pthread_join(g_worker.thread, NULL);
        return false;
    }
    return true;
}

EXPORT int32_t
rvn_sync_pager(void *handle,
               int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    if (!io_ring_setup_successful())
    {
        if (_flush_file(handle_ptr->file_fd))
        {
            *detailed_error_code = errno;
            return FAIL_SYNC_FILE;
        }
        return SUCCESS;
    }
    struct workitem work =
        {
            .iovec_count = 0,
            .iovecs = NULL,
            .completed = 0,
            .type = workitem_fsync,
            .filefd = handle_ptr->file_fd,
            .offset = 0,
            .errored = false,
            .result = 0,
            .notifyfd = handle_ptr->global_state->eventfd,
        };
    queue_work(&work);
    if (!wake_ring_worker(detailed_error_code))
    {
        return FAIL_IO_RING_WRITE;
    }
    struct pollfd pfd = {.fd = handle_ptr->global_state->eventfd, .events = POLLIN};
    while (!atomic_load(&work.completed))
    {
        if (poll(&pfd, 1, -1) == -1)
        {
            if (errno == EINTR)
                continue;
            *detailed_error_code = errno;
            return FAIL_POLL_EVENTFD;
        }
    }
    if (work.errored)
    {
        *detailed_error_code = work.result;
        return FAIL_IO_RING_WRITE_RESULT;
    }
    return SUCCESS;
}

int32_t rvn_write_io_ring(
    void *handle,
    struct page_to_write *buffers,
    int32_t count,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;
    if (count == 0)
        return SUCCESS;

    if (g_worker.errored)
    {
        *detailed_error_code = g_worker.result;
        return FAIL_IO_RING_SUBMIT;
    }

    pthread_mutex_lock(&handle_ptr->global_state->lock);
    // The worst case is that we have separate work item for each buffer, we assume that we
    // can usually do better, though..
    size_t max_req_size = (size_t)count * (sizeof(struct iovec) + sizeof(struct workitem));
    if (handle_ptr->global_state->arena_size < max_req_size)
    {
        size_t size = nextPowerOf2(max_req_size);
        void *ptr = realloc(handle_ptr->global_state->arena, size);
        if (!ptr)
        {
            *detailed_error_code = errno;
            pthread_mutex_unlock(&handle_ptr->global_state->lock);
            return FAIL_NOMEM;
        }
        handle_ptr->global_state->arena = ptr;
        handle_ptr->global_state->arena_size = size;
    }
    void *buf = handle_ptr->global_state->arena;
    struct workitem *prev = NULL;
    for (int32_t curIdx = 0; curIdx < count; curIdx++)
    {
        int64_t offset = buffers[curIdx].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[curIdx].count_of_pages * VORON_PAGE_SIZE;
        int64_t after = offset + size;

        struct workitem *work = buf;
        *work = (struct workitem){
            .iovec_count = 1,
            .iovecs = buf + sizeof(struct workitem),
            .completed = 0,
            .type = workitem_write,
            .filefd = handle_ptr->file_fd,
            .offset = offset,
            .errored = false,
            .result = 0,
            .prev = prev,
            .notifyfd = handle_ptr->global_state->eventfd,
        };
        prev = work;
        work->iovecs[0] = (struct iovec){.iov_len = size, .iov_base = buffers[curIdx].ptr};
        buf += sizeof(struct workitem) + sizeof(struct iovec);

        for (size_t nextIndex = curIdx + 1; nextIndex < count && work->iovec_count < IOV_MAX; nextIndex++)
        {
            int64_t dest = buffers[nextIndex].page_num * VORON_PAGE_SIZE;
            if (after != dest)
                break;

            size = (int64_t)buffers[nextIndex].count_of_pages * VORON_PAGE_SIZE;
            after = dest + size;
            work->iovecs[work->iovec_count++] = (struct iovec){.iov_base = buffers[nextIndex].ptr, .iov_len = size};
            curIdx++;
            buf += sizeof(struct iovec);
        }
        queue_work(work);
    }

    if (!wake_ring_worker(detailed_error_code))
    {
        pthread_mutex_unlock(&handle_ptr->global_state->lock);
        return FAIL_IO_RING_WRITE;
    }

    struct pollfd pfd = {.fd = handle_ptr->global_state->eventfd, .events = POLLIN};
    bool all_done = false;
    while (!all_done)
    {
        all_done = true;
        rc = SUCCESS;
        *detailed_error_code = 0;

        if (poll(&pfd, 1, -1) == -1)
        {
            if (errno == EINTR)
                continue;
            *detailed_error_code = errno;
            rc = FAIL_POLL_EVENTFD;
            break;
        }

        struct workitem *work = prev;
        while (work)
        {
            all_done &= atomic_load(&work->completed);
            if (work->errored)
            {
                *detailed_error_code = work->result;
                rc = FAIL_IO_RING_WRITE_RESULT;
                // note that we still need to wait for the whole
                // set to complete before we can safely return...
            }
            // move to the previous one...
            work = work->prev;
        }
    }

    pthread_mutex_unlock(&handle_ptr->global_state->lock);
    return rc;
}

PRIVATE int32_t
rvn_one_time_init(int32_t *detailed_error_code)
{
    if (g_cfg.io_ring_queue_size < 0 ||
        !_io_ring_supported())
        return SUCCESS; // not supported or disabled, that is fine...

    struct io_uring_params params = {.flags = 0};

    int rc = io_uring_queue_init_params(g_cfg.io_ring_queue_size, &g_worker.ring, &params);
    if (rc)
    {
        // if we were expclitly asked to use io ring, we fail
        if (g_cfg.write_mode == rvn_write_mode_io_ring)
        {
            *detailed_error_code = -rc;
            return FAIL_CREATE_IO_RING;
        }
        // we tried, but failed, so we'll use vectored I/O instead
        g_cfg.write_mode = rvn_write_mode_vectored_file_io;
        g_worker = (struct worker){.eventfd = -1, .ring = {.ring_fd = -1}};
        return SUCCESS;
    }
    g_worker.eventfd = eventfd(0, EFD_CLOEXEC);
    if (g_worker.eventfd == -1)
    {
        *detailed_error_code = errno;
        rc = FAIL_CREATE_EVENTFD;
        goto error;
    }
    rc = io_uring_register_eventfd(&g_worker.ring, g_worker.eventfd);
    if (rc)
    {
        *detailed_error_code = -rc;
        rc = FAIL_IO_RING_REG_EVENTFD;
        goto error;
    }

    int result = pthread_create(&g_worker.thread, NULL, do_ring_work, NULL);
    if (result != 0)
    {
        *detailed_error_code = result;
        rc = FAIL_CREATE_THREAD;
        goto error;
    }
    return SUCCESS;
error:
    io_uring_queue_exit(&g_worker.ring);
    if (g_worker.eventfd != -1)
        close(g_worker.eventfd);
    g_worker = (struct worker){
        .eventfd = -1,
        .errored = true,
        .result = rc,
        .ring = {.ring_fd = -1},
    };
    return rc;
}

int io_ring_setup_successful()
{
    return g_worker.ring.ring_fd != -1;
}
