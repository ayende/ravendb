// Tests the prefetch ring's submitter when a submit fails (needs io_uring, so a kernel and a container that allow it).
//
// Linux: zig cc -Wall -O1 -g -DRVN_TEST_HOOKS -Iinc -target x86_64-linux-gnu.2.17 -o test-prefetch test-prefetch.c
//          src/posix/prefetch.c src/posix/syscalls.c ../../libs/liburing/liburing-2.8.1-x64.a -lpthread
// Run:   test-prefetch <directory>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

extern _Atomic int32_t rvn_test_failing_submits;
extern _Atomic int32_t rvn_test_submit_error;

#define CHECK(cond, ...)                                         \
    do                                                           \
    {                                                            \
        if (!(cond))                                             \
        {                                                        \
            fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
            fprintf(stderr, __VA_ARGS__);                        \
            fprintf(stderr, "\n");                               \
            exit(1);                                             \
        }                                                        \
    } while (0)

static _Atomic int g_done[16];

static void
on_reads_done(void *context, const uint32_t *groups, int32_t count)
{
    (void)context;
    for (int32_t i = 0; i < count; i++)
        atomic_fetch_add(&g_done[groups[i]], 1);
}

static int
wait_done(uint32_t group, int ms)
{
    for (int i = 0; i < ms; i++)
    {
        if (atomic_load(&g_done[group]) != 0)
            return 1;
        struct timespec t = {0, 1000000};
        nanosleep(&t, NULL);
    }
    return atomic_load(&g_done[group]) != 0;
}

static void
read_group(void *ring, int64_t ticket, uint32_t group)
{
    int64_t offsets[4] = {0, 8192, 5 * 8192, 9 * 8192};
    CHECK(rvn_prefetch_ring_read_pages(ring, ticket, offsets, 4, group) == 1, "read_pages");
}

int main(int argc, char **argv)
{
    CHECK(argc == 2, "usage: test-prefetch <directory>");
    char path[512];
    snprintf(path, sizeof(path), "%s/prefetch-test.data", argv[1]);
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    CHECK(fd != -1, "open %s: %d", path, errno);
    char page[8192];
    memset(page, 7, sizeof(page));
    for (int i = 0; i < 16; i++)
        CHECK(write(fd, page, sizeof(page)) == sizeof(page), "write");

    void *ring;
    int32_t err;
    CHECK(rvn_prefetch_ring_create(8, 4, 64, 16, on_reads_done, NULL, &ring, &err) == SUCCESS, "create %d", err);
    struct rvn_prefetch_ring_stats stats;
    rvn_prefetch_ring_stats(ring, &stats);
    if (stats.uses_io_uring == 0)
    {
        printf("SKIPPED: no io_uring here\n");
        return 0;
    }
    int64_t ticket;
    CHECK(rvn_prefetch_register(ring, fd, &ticket, &err) == SUCCESS, "register %d", err);

    /* a few submits fail with nothing in flight: the submitter retries, the group completes with its reads */
    atomic_store(&rvn_test_submit_error, -EAGAIN);
    atomic_store(&rvn_test_failing_submits, 5);
    read_group(ring, ticket, 1);
    CHECK(wait_done(1, 5000), "the group did not complete after 5 failed submits");
    rvn_prefetch_ring_stats(ring, &stats);
    CHECK(stats.reads == 4 && stats.failed == 0, "reads %lld failed %lld", (long long)stats.reads, (long long)stats.failed);
    printf("transient failures: OK\n");

    /* submits keep failing: the group waits, then completes once a submit works */
    atomic_store(&rvn_test_failing_submits, 1000000);
    read_group(ring, ticket, 2);
    CHECK(wait_done(2, 300) == 0, "the group completed while every submit failed");
    atomic_store(&rvn_test_failing_submits, 0);
    CHECK(wait_done(2, 5000), "the group did not complete once submits work again");
    printf("long failure, then recovery: OK\n");

    /* a failure that does not clear: the group completes without reads, and so do the next ones */
    atomic_store(&rvn_test_submit_error, -EBADF);
    atomic_store(&rvn_test_failing_submits, 1);
    read_group(ring, ticket, 3);
    CHECK(wait_done(3, 5000), "the group did not complete after the ring failed");
    rvn_prefetch_ring_stats(ring, &stats);
    CHECK(stats.failed >= 4, "failed %lld", (long long)stats.failed);
    read_group(ring, ticket, 4);
    CHECK(wait_done(4, 5000), "a group after the ring failed did not complete");
    printf("failed ring: OK\n");

    rvn_prefetch_unregister(ring, ticket);
    CHECK(rvn_prefetch_ring_destroy(ring, &err) == SUCCESS, "destroy %d", err);

    /* stopping while submits fail: the group still completes */
    memset(g_done, 0, sizeof(g_done));
    CHECK(rvn_prefetch_ring_create(8, 4, 64, 16, on_reads_done, NULL, &ring, &err) == SUCCESS, "create %d", err);
    CHECK(rvn_prefetch_register(ring, fd, &ticket, &err) == SUCCESS, "register %d", err);
    atomic_store(&rvn_test_submit_error, -EAGAIN);
    atomic_store(&rvn_test_failing_submits, 1000000);
    read_group(ring, ticket, 5);
    CHECK(wait_done(5, 200) == 0, "the group completed while every submit failed");
    rvn_prefetch_unregister(ring, ticket);
    CHECK(rvn_prefetch_ring_destroy(ring, &err) == SUCCESS, "destroy %d", err);
    CHECK(atomic_load(&g_done[5]) == 1, "the group did not complete when the ring stopped");
    atomic_store(&rvn_test_failing_submits, 0);
    printf("stop while failing: OK\n");

    close(fd);
    unlink(path);
    printf("ALL OK\n");
    return 0;
}
