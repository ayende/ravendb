#if defined(__unix__) && !defined(__APPLE__)

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/utsname.h>
#include <unistd.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <sys/syscall.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>

#include "rvn.h"
#include "rvn_internal.h"
#include "status_codes.h"
#include "internal_posix.h"

#define IO_RING_SIZE (256)


EXPORT uint64_t
rvn_get_current_thread_id(void)
{
  return syscall(SYS_gettid);
}

PRIVATE int32_t
_flush_file(int32_t fd)
{
  return fsync(fd);
}

#define SMB2_MAGIC_NUMBER 0xfe534d42

PRIVATE int32_t
_sync_directory_allowed(int32_t dir_fd)
{
  struct statfs buf;
  if (fstatfs(dir_fd, &buf) == -1)
    return FAIL;

  switch (buf.f_type)
  {
  case NFS_SUPER_MAGIC:
  case CIFS_MAGIC_NUMBER:
  case SMB_SUPER_MAGIC:
  case SMB2_MAGIC_NUMBER:
    return SYNC_DIR_NOT_ALLOWED;
  default:
    return SYNC_DIR_ALLOWED;
  }
}

PRIVATE int32_t
_finish_open_file_with_odirect(int32_t fd)
{
  /* nothing to do in posix, O_DIRECT is supported */
  return 0;
}

PRIVATE int32_t
_rvn_fallocate(int32_t fd, int64_t offset, int64_t size)
{
  return posix_fallocate64(fd, offset, size);
}

PRIVATE char*
_get_strerror_r(int32_t error, char* tmp_buff, int32_t buf_size)
{
  return strerror_r(error, tmp_buff, buf_size);
}

EXPORT int32_t
rvn_test_storage_durability(
    const char *temp_file_name,
    int32_t *detailed_error_code)
{
    int fd = open(temp_file_name, O_WRONLY | O_DSYNC | O_DIRECT | O_CREAT, S_IWUSR | S_IRUSR);
    int rc = SUCCESS;
    if (fd == -1)
    {
        rc = FAIL_OPEN_FILE;
        goto error_cleanup;
    }

    rc = _allocate_file_space(fd, 64 * 1024, detailed_error_code);
    if ( rc != SUCCESS )
    {
        rc = FAIL_ALLOC_FILE;
        if ( errno == EINVAL)
            rc = FAIL_TEST_DURABILITY;
        goto error_cleanup;        
    }

error_cleanup:
    if (rc != SUCCESS)
        *detailed_error_code = errno;
    if (fd != -1)
    {
        close(fd);
        unlink(temp_file_name);
    }

    return rc;
}

int _io_ring_supported_internal()
{
  
  if(sizeof(void*) != 8)
    return 0;

   struct utsname buffer; 
   if (uname(&buffer) != 0)
        return 0;

  int curr_major = 0, curr_minor = 0, curr_patch = 0;
  sscanf(buffer.release, "%d.%d.%d", &curr_major, &curr_minor, &curr_patch); 
  
  return curr_major > 5 || (curr_major==5 && curr_minor>=1);
}

static bool _io_ring_supported_flag = false;
static bool _io_ring_supported_cached = false;

bool _io_ring_supported()
{
    if(_io_ring_supported_cached)
        return _io_ring_supported_flag == 1;
    _io_ring_supported_flag = _io_ring_supported_internal();
    _io_ring_supported_cached = true;
    return _io_ring_supported_flag;
}


int32_t _setup_io_ring(struct handle_global_state *global_state, int32_t *detailed_error_code)
{
    int rc = io_uring_queue_init(IO_RING_SIZE, &global_state->ring, 0);
    if(rc)
    {
        *detailed_error_code = -rc;
        return FAIL_CREATE_IO_RING;
    }
    return SUCCESS;
}

void _close_io_ring(struct handle_global_state *global_state)
{
    io_uring_queue_exit(&global_state->ring);
}

/*
* The idea here that we will submit the writes in a batch, and then wait for them to complete.
* However, we need to deal with partial writes, as well as batches that are greater than the capacity
* of the ring. To handle that, we keep track of all the offsets that we wrote right now, and scan through
* both the offsets and the submitted pages, then we can adjust the writes offsets to retry again if there
* is a partial write.
* This process also handles the case where the ring is full, and we need to flush it, using the same logic.
*/
int32_t _submit_writes_to_ring(
    struct handle *handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t* detailed_error_code)
{
    struct io_uring *ring = &handle->global_state->ring;
    off_t *offsets = handle->global_state->offsets;
    memset(offsets, 0, count * sizeof(off_t));   

    while(true)
    {
        int32_t submitted = 0;

        for (size_t i = 0; i < count; i++)
        {
            off_t offset =  offsets[i];
            if(offset == buffers[i].count_of_pages * VORON_PAGE_SIZE)
                continue;

            struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
            if (sqe == NULL) // the ring is full, flush it...
                break;

            io_uring_sqe_set_data64(sqe, i);
            io_uring_prep_write(sqe, handle->file_fd, 
                buffers[i].ptr + offset,
                buffers[i].count_of_pages * VORON_PAGE_SIZE - offset,
                buffers[i].page_num * VORON_PAGE_SIZE + offset);

            submitted++;
        }

        if(submitted == 0)
            return SUCCESS;
        
        int32_t rc = io_uring_submit_and_wait(ring, submitted);
        if(rc != 0)
        {
            *detailed_error_code = -rc;
            return FAIL_IO_RING_SUBMIT;
        }

        struct io_uring_cqe *cqe;
        uint32_t head = 0;
        uint32_t i = 0;
        bool has_errors = false;

        io_uring_for_each_cqe(ring, head, cqe) {
            i++;
            uint64_t index = io_uring_cqe_get_data64(cqe);
            int result = cqe->res;
            if(result < 0)
            {
                has_errors = true;
                *detailed_error_code = -result;
            }
            else
            {
                offsets[index] += result;
                if(result == 0)
                {
                    // there shouldn't be a scenario where we return 0 
                    // for a write operation, we may want to retry here
                    // but figuring out if this is a single happening, of if 
                    // we need to retry this operation (or _have_ retried it) is 
                    // complex enough to treat this as an error for now.
                    has_errors = true;
                    *detailed_error_code = EIO;   
                }
            }
        }
        io_uring_cq_advance(ring, i);

        if(has_errors)
            return FAIL_IO_RING_WRITE_RESULT;
    }
}

int32_t _ensure_offsets_size(struct handle *handle_ptr, size_t count, int32_t *detailed_error_code)
{
    if(count > handle_ptr->global_state->offsets_count)
    {
        size_t total_size = count * sizeof(off_t);
        if (total_size / sizeof(off_t) != count) {
            *detailed_error_code = EOVERFLOW;
            return FAIL_MATH_OVERFLOW;
            
        } 
        void* buf = realloc(handle_ptr->global_state->offsets, total_size);
        if(!buf){
            *detailed_error_code = ENOMEM;
            return FAIL_NOMEM;
        }
        handle_ptr->global_state->offsets_count = count;
        handle_ptr->global_state->offsets = buf;

    }
    return SUCCESS;
}

int32_t rvn_write_io_ring(
    void *handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code)
{
    int32_t rc = SUCCESS;
    struct handle *handle_ptr = handle;

    pthread_mutex_lock(&handle_ptr->global_state->lock);

    rc = _ensure_offsets_size(handle_ptr, count, detailed_error_code);
    if(rc == SUCCESS)
    {
        rc = _submit_writes_to_ring(handle_ptr, count, buffers, detailed_error_code);
    }

    pthread_mutex_unlock(&handle_ptr->global_state->lock);
    return rc;
}


PRIVATE
int32_t rvn_write_vectored_file_io(
    void* handle,
    int32_t count,
    struct page_to_write *buffers,
    int32_t *detailed_error_code
)
{
    struct iovec iovs[IOV_MAX];
    int used = 0;

    struct handle *handle_ptr = handle;
    for(int32_t curIdx = 0; curIdx < count; curIdx++)
    {
        int64_t offset = buffers[curIdx].page_num * VORON_PAGE_SIZE;
        int64_t size = (int64_t)buffers[curIdx].count_of_pages * VORON_PAGE_SIZE;
        int64_t after = offset+size;
        iovs[0].iov_base = buffers[curIdx].ptr;
        iovs[0].iov_len = size;
        used = 1;

        for (size_t nextIndex = curIdx + 1; nextIndex < count && used < IOV_MAX; nextIndex++)
        {
            int64_t dest = buffers[nextIndex].page_num * VORON_PAGE_SIZE;
            if (after != dest)
                break;
                
            size = (int64_t)buffers[nextIndex].count_of_pages * VORON_PAGE_SIZE;
            after = dest + size;
            iovs[used].iov_base = buffers[nextIndex].ptr;
            iovs[used].iov_len = size;
            used++;
            curIdx++;
        }

        int32_t rc = _pwritev(handle_ptr->file_fd, iovs, used, offset, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }
    return SUCCESS;
}

EXPORT
rvn_writer rvn_get_writer(void* handle)
{
    struct handle *handle_ptr = handle;

    if(handle_ptr->write_address)
        return rvn_write_mmap;
    if(handle_ptr->global_state->ring.ring_fd != -1)
        return rvn_write_io_ring;

    rvn_write_mode mode = _get_writer_mode();
    switch (mode)
    {
        case rvn_write_mode_vectored_file_io:
            return rvn_write_vectored_file_io;
        case rvn_write_mode_file_io:
            return rvn_write_file_io;
        case rvn_write_mode_io_ring:
           return rvn_write_invalid_setup;
        case rvn_write_mode_mmap:
            return rvn_write_invalid_setup;
        default:
            return rvn_write_vectored_file_io;
    }
}


EXPORT int32_t
rvn_write_journal(void* handle, struct journal_entry* buffer, int64_t count_of_entries, int64_t offset, int32_t* detailed_error_code)
{
    struct journal_handle* jfh = handle;
    struct iovec elements[IOV_MAX];
    int32_t index = 0;
    for (size_t i = 0; i < count_of_entries; i++)
    {
        elements[index].iov_base = buffer[i].base;
        elements[index].iov_len = buffer[i].number_of_4kbs * SYS_PAGE_SIZE;
        if(elements[index].iov_len / SYS_PAGE_SIZE != buffer[i].number_of_4kbs)
        {
            *detailed_error_code = EOVERFLOW;
            return FAIL_MATH_OVERFLOW;
        }
        index++;
        if(index == IOV_MAX)
        {
            int32_t rc = _pwritev(jfh->fd, elements, index, offset, detailed_error_code);
            if(rc != SUCCESS)
                return rc;
            offset += index * SYS_PAGE_SIZE;
            index = 0;
        }
    }
    if(index > 0)
    {
        int32_t rc = _pwritev(jfh->fd, elements, index, offset, detailed_error_code);
        if(rc != SUCCESS)
            return rc;
    }
    return SUCCESS;
}

#endif