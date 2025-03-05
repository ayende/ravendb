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
        .io_ring_queue_size = 4,
        .low_priority_io = false,
        .write_mode = rvn_write_mode_io_ring };
    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);

    rc = rvn_init_pager(L"test.db", 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);
    char buf[8192] = { 0 };
    buf[1] = 'a';
    for (size_t x = 0; x < 10; x++)
    {
        struct page_to_write* p = calloc(34, sizeof(struct page_to_write));
        for (size_t i = 0; i < 34; i++)
        {
            p[i].count_of_pages = 1;
            p[i].ptr = buf;
            p[i].page_num = i;
        }
        rc = rvn_write_io_ring(handle, p, 34, &err);
    }
    
    printf("done\n");
    return 0;
}
