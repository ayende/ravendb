#include "rvn.h"
#include <assert.h>
#include "status_codes.h"
#include <stdio.h>
#include <windows.h>
#include "internal_win.h"
#include <dbghelp.h>

#pragma comment(lib, "DbgHelp.lib")

typedef struct {
    int32_t len;
    struct page_to_write* pages;
} LenPageData;

LenPageData* read_page_data(const char* filename, char* buf) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open file");
        return NULL;
    }

    // First pass: count number of lines to allocate memory
    int line_count = 0;
    char buffer[1024 * 128];
    while (fgets(buffer, sizeof(buffer), file)) {
        if (strlen(buffer) > 1) {  // Ignore empty lines
            line_count++;
        }
    }
    rewind(file);

    // Allocate array for results
    LenPageData* result = calloc(line_count + 1, sizeof(LenPageData));
    if (!result) {
        fclose(file);
        return NULL;
    }

    // Second pass: parse the data
    int current_line = 0;
    while (fgets(buffer, sizeof(buffer), file) && current_line < line_count) {
        // Remove trailing newline
        buffer[strcspn(buffer, "\n")] = 0;

        if (strlen(buffer) <= 1) {
            continue;
        }

        // Parse LEN
        long len;
        char* ptr = buffer;
        if (sscanf_s(ptr, "%ld -", &len) != 1) {
            continue;
        }
        result[current_line].len = len;

        // Allocate memory for pairs
        result[current_line].pages = malloc(len * sizeof(struct page_to_write));


        // Parse PageNum:Count pairs
        int pair_index = 0;
        ptr = strchr(buffer, '-') + 1;
        char* context = NULL;
        char* token = strtok_s(ptr, ",", &context);
        while (token && pair_index < len) {
            long page_num, count;
            if (sscanf_s(token, "%ld:%ld", &page_num, &count) == 2) {
                result[current_line].pages[pair_index].page_num = page_num;
                result[current_line].pages[pair_index].count_of_pages = count;
                if (count != 1) {
                    printf("fFFF");
                }
                result[current_line].pages[pair_index].ptr = ptr;
                pair_index++;
            }
            token = strtok_s(NULL, ",", &context);
        }

        current_line++;
    }

    fclose(file);

    return result;
}

char* path_combine(const char* path1, const char* path2) {
    if (!path1 || !path2) return NULL;

    size_t len1 = strlen(path1);
    size_t len2 = strlen(path2);
    if (len1 == 0) return _strdup(path2);
    if (len2 == 0) return _strdup(path1);

    // Check if path1 ends with a separator
    BOOL needs_separator = (path1[len1 - 1] != '\\' && path1[len1 - 1] != '/');

    // Allocate buffer: path1 + separator (if needed) + path2 + null terminator
    size_t total_len = len1 + len2 + (needs_separator ? 1 : 0);
    char* result = (char*)malloc(total_len + 1);
    if (!result) return NULL;

    // Build the combined path
    strcpy_s(result, total_len + 1, path1);
    if (needs_separator) {
        strcat_s(result, total_len + 1, "\\");
    }
    strcat_s(result, total_len + 1, path2);

    return result;
}

wchar_t* get_temp_file_name() {
    wchar_t temp_path[MAX_PATH];
    wchar_t* temp_file = NULL;

    // Get the temp directory path
    DWORD path_len = GetTempPathW(MAX_PATH, temp_path);
    if (path_len == 0 || path_len > MAX_PATH) {
        wprintf(L"Failed to get temp path: %lu\n", GetLastError());
        return NULL;
    }

    // Generate a unique temp file name in that path
    temp_file = (wchar_t*)malloc(MAX_PATH * sizeof(wchar_t));
    if (!temp_file) {
        return NULL;
    }

    if (GetTempFileNameW(temp_path, L"DB", 0, temp_file) == 0) {
        wprintf(L"Failed to get temp file name: %lu\n", GetLastError());
        free(temp_file);
        return NULL;
    }

    return temp_file;
}

char** get_files_in_directory(const char* dir_path) {
    char search_path[MAX_PATH];
    snprintf(search_path, MAX_PATH, "%s\\*.*", dir_path); // Append "\*.*" for all files

    WIN32_FIND_DATAA find_data;
    HANDLE hFind = FindFirstFileA(search_path, &find_data);
    if (hFind == INVALID_HANDLE_VALUE) {
        if (GetLastError() != ERROR_FILE_NOT_FOUND) {
            printf("Error opening directory: %lu\n", GetLastError());
        }
        char** empty = (char**)malloc(sizeof(char*));
        if (empty) empty[0] = NULL; // Return empty list
        return empty;
    }

    // Dynamically growable array
    char** files = NULL;
    size_t capacity = 0;
    size_t count = 0;

    do {
        if (!(find_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
            // Resize array if needed
            if (count >= capacity) {
                capacity = capacity ? capacity * 2 : 8; // Start with 8, then double
                char** temp = (char**)realloc(files, (capacity + 1) * sizeof(char*));
                if (!temp) {
                    for (size_t i = 0; i < count; i++) free(files[i]);
                    free(files);
                    FindClose(hFind);
                    return NULL;
                }
                files = temp;
            }

            // Add file name
            files[count] = path_combine(dir_path, find_data.cFileName);
            if (!files[count]) {
                for (size_t i = 0; i < count; i++) free(files[i]);
                free(files);
                FindClose(hFind);
                return NULL;
            }
            count++;
        }
    } while (FindNextFileA(hFind, &find_data));

    FindClose(hFind);

    // Ensure null-terminated
    char** final = (char**)realloc(files, (count + 1) * sizeof(char*));
    if (!final) {
        for (size_t i = 0; i < count; i++) free(files[i]);
        free(files);
        return NULL;
    }
    final[count] = NULL; // Null terminator
    return final;
}


void once(LenPageData* log)
{
    void* handle;
    void* mem;
    void* wmem;
    int64_t size;
    int32_t err;

    wchar_t* db = get_temp_file_name();

    int rc = rvn_init_pager(db, 1024 * 64, OPEN_FILE_WRITABLE_MAP, &handle, &mem, &wmem, &size, &err);

    LenPageData* c = log;
    while (c->len) {
        rc = rvn_write_io_ring(handle, c->pages, c->len, &err);
        c++;
    }

    rvn_close_pager(handle, &err);
    DeleteFileW(db);

}

int thread(void* state)
{
    for (size_t i = 0; i < 100; i++)
    {
        LenPageData** files = state;
        while (*files)
        {
            once(*files);
            files++;
        }

        DWORD threadId = GetCurrentThreadId();

        // Get current time
        SYSTEMTIME st;
        GetLocalTime(&st);

        // Print thread ID and time
        printf("%lu @ %02d/%02d/%04d %02d:%02d:%02d.%03d\n",
            threadId,
            st.wDay, st.wMonth, st.wYear,
            st.wHour, st.wMinute, st.wSecond, st.wMilliseconds);
    }

    return 0;
}

int main()
{
    char buf[8192] = { 0 };
    buf[1] = 'a';

    struct rvn_configuration cfg = {
        .io_ring_queue_size = 4,
        .low_priority_io = false,
        .write_mode = rvn_write_mode_io_ring };
    int32_t ec;
    int32_t rc = rvn_startup_configure(&cfg, &ec);

    char** files = get_files_in_directory("C:\\Work\\ravendb-7.0\\src\\Raven.Pal\\tmp");
    LenPageData** log = NULL;
    size_t count = 0;
    while (*files)
    {
        log = realloc(log, sizeof(LenPageData*) * (count + 1));
        log[count++] = read_page_data(*files, buf);
        files++;
    }
    log = realloc(log, sizeof(LenPageData*) * (count + 1));
    log[count] = 0;

    const int num_threads = 32;
    HANDLE threads[32];

    // Create 4 threads
    for (int i = 0; i < num_threads; i++) {
        threads[i] = CreateThread(
            NULL,           // Default security attributes
            0,              // Default stack size
            thread,     // Thread function
            log,           // No parameter passed to thread
            0,              // Run immediately
            NULL            // No need for thread ID
        );
        if (threads[i] == NULL) {
            printf("Failed to create thread %d: %lu\n", i, GetLastError());
            // Cleanup already created threads
            for (int j = 0; j < i; j++) {
                CloseHandle(threads[j]);
            }
            return 1;
        }
    }

    // Wait for all threads to complete
    auto r = WaitForMultipleObjects(
        num_threads,    // Number of threads
        threads,        // Array of thread handles
        TRUE,           // Wait for all to finish
        INFINITE        // No timeout
    );

    // Clean up thread handles
    for (int i = 0; i < num_threads; i++) {
        CloseHandle(threads[i]);
    }

    printf("All threads completed\n");
    return 0;
    return 0;
}
