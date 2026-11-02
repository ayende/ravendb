#if defined(__unix__) && !defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/uio.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "internal_posix.h"

/* Raw system calls: glibc's preadv2 needs glibc 2.26 (the PAL needs only 2.17) */

#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif

PRIVATE ssize_t
_preadv2_nowait(int fd, void *buffer, size_t length, int64_t offset)
{
    struct iovec iov = { .iov_base = buffer, .iov_len = length };
#if __SIZEOF_LONG__ == 8
    return syscall(SYS_preadv2, fd, &iov, 1, (long)offset, 0L, RWF_NOWAIT);
#else
    return syscall(SYS_preadv2, fd, &iov, 1, (long)(uint32_t)offset, (long)((uint64_t)offset >> 32), RWF_NOWAIT);
#endif
}

#endif
