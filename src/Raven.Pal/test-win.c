#include "rvn.h"
#include <assert.h>
#include "status_codes.h"
#include <stdio.h>
#include <windows.h>
#include "internal_win.h"

int main()
{
    void* handle;
    void* mem;
    void* wmem;
    int64_t size;
    int32_t err;

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 16,
        .low_priority_io = true,
        .write_mode = rvn_write_mode_io_ring };
    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);

    rc = rvn_init_pager("test.db", 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    char buf[8192] = { 0 };
    buf[1] = 'a';
    struct page_to_write p = { .count_of_pages = 1, .page_num = 0, .ptr = buf };
    rc = rvn_write_io_ring(handle, &p, 1, &err);
    return 0;
}
