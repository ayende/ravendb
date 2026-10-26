#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <assert.h>

#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

EXPORT int32_t
rvn_prefetch_ranges(struct RVN_RANGE_LIST *range_list, int32_t count, int32_t *detailed_error_code)
{
    // the kernel reads at most max(io_pages, ra_pages) of a WILLNEED request (128-256 KB on common devices) and drops the
    // rest, so a range goes out as several requests
    const size_t request_size = 128 * 1024;
    int32_t i = 0;
    for (i = 0; i < count; i++)
    {
        struct RVN_RANGE_LIST record = range_list[i];
        for (size_t done = 0; done < record.number_of_bytes; done += request_size)
        {
            size_t size = record.number_of_bytes - done < request_size ? record.number_of_bytes - done : request_size;
            if (madvise((char *)record.virtual_address + done, size, MADV_WILLNEED) != 0)
            {
                *detailed_error_code = errno;
                return FAIL;
            }
        }
    }
    return SUCCESS;
}
