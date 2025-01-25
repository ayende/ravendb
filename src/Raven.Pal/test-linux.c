#include <stdio.h>
#include "rvn.h"
#include "status_codes.h"
#include "internal_posix.h"

//  The command to build and run this is:
//  /home/ayende/zig-linux-x86_64-0.14.0-dev.2851+b074fb7dd/zig cc -Wall -O0 -g -fPIC -Iinc -target x86_64-linux-gnu ../../libs/liburing/liburing-2.8.1-x64.a -o test src/shared_all.c src/rvngetpalver.c src/posix/fileutils.c src/posix/geterrorstring.c src/posix/getsysteminformation.c src/posix/journal.c src/posix/mapping.c src/posix/pager.c src/posix/sync.c src/posix/virtualmemory.c src/posix/writefileheader.c src/posix/linuxonly.c test-linux.c
int main()
{
    void *handle;
    void *mem;
    void *wmem;
    int64_t size;
    int32_t err;

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 16,
        .low_priority_io = true,
        .write_mode = rvn_write_mode_io_ring};
    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);

    rc = rvn_init_pager("test.db", 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    char buf[8192] = {0};
    buf[1] = 'a';
    struct page_to_write p = {.count_of_pages = 1, .page_num = 0, .ptr = buf};
    rc = rvn_write_io_ring(handle, &p, 1, &err);
    return 0;
}
