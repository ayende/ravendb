/* Dirty-range tracking + paced writeback, shared between the posix and win
   pagers. Included from posix/pager.c and win/pager.c AFTER the platform
   headers, so `struct handle` / `struct handle_global_state` (which share the
   relevant field names) are already defined. The platform supplies:

     _writeback_supported(handle)               - can this pager push ranges?
     _writeback_range_start(handle, off, len)   - initiate writeback of a range
     _writeback_range_complete(handle, off, len)- wait for it (no-op where the
                                                  start call is synchronous)
   (linux: linuxonly.c, mac: maconly.c, windows: win/pager.c)

   Tracking is inferred from the open flags: every writable, persistent file is
   tracked (which covers recovery-time writes to the data file for free);
   temporary and read-only/copy-on-write pagers are not. The bitmap only
   materializes on the first write, so pagers that never write cost nothing.

   The bitmap is a pacing hint, never a durability structure: a lost or stale
   bit only means the range rides the final fdatasync unpaced, so the racy
   plain read-modify-write below is safe by construction. Bits are set by the
   writers (single writer thread per file, guaranteed by Voron) and consumed by
   rvn_pager_writeback_dirty (any thread) via atomic exchange - the exchange is
   the one op that must be atomic, so concurrent drains self-partition and a
   drain never pushes a range twice. A marker racing the exchange can at worst
   resurrect a just-consumed bit (one redundant push) or lose a fresh bit to a
   concurrent put-back (the page stays dirty and the barrier still covers it). */

#if !defined(_WIN32)
#include <time.h>
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#define rvn_atomic_xchg64(p, v) InterlockedExchange64((volatile LONG64 *)(p), (LONG64)(v))
#define rvn_atomic_load_ptr(p) InterlockedCompareExchangePointer((PVOID volatile *)(p), NULL, NULL)
#define rvn_atomic_store_ptr(p, v) InterlockedExchangePointer((PVOID volatile *)(p), (v))
static int32_t _wb_ctz64(uint64_t v)
{
    unsigned long i;
    _BitScanForward64(&i, v);
    return (int32_t)i;
}
#define _wb_popcnt64(v) ((int32_t)__popcnt64(v))
#else
#define rvn_atomic_xchg64(p, v) __atomic_exchange_n((p), (v), __ATOMIC_ACQ_REL)
#define rvn_atomic_load_ptr(p) __atomic_load_n((p), __ATOMIC_ACQUIRE)
#define rvn_atomic_store_ptr(p, v) __atomic_store_n((p), (v), __ATOMIC_RELEASE)
#define _wb_ctz64(v) ((int32_t)__builtin_ctzll(v))
#define _wb_popcnt64(v) ((int32_t)__builtin_popcountll(v))
#endif

#define WRITEBACK_MIN_BITMAP_WORDS 128 /* 1KB of bitmap = 8GB of file */

struct dirty_bitmap
{
    int64_t number_of_words;
    struct dirty_bitmap *prev; /* retired generations, freed with global_state */
    uint64_t words[];
};

PRIVATE int32_t
_writeback_supported(struct handle *handle_ptr);

PRIVATE int32_t
_writeback_range_start(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code);

PRIVATE int32_t
_writeback_range_complete(struct handle *handle_ptr, int64_t offset, int64_t length, int32_t *detailed_error_code);

static bool
_should_track_dirty_ranges(int32_t open_flags)
{
    return (open_flags & (OPEN_FILE_TEMPORARY | OPEN_FILE_READ_ONLY | OPEN_FILE_COPY_ON_WRITE)) == 0;
}

PRIVATE void
_free_dirty_bitmaps(struct dirty_bitmap *bm)
{
    while (bm != NULL)
    {
        struct dirty_bitmap *prev = bm->prev;
        free(bm);
        bm = prev;
    }
}

static int64_t
_wb_now_micros(void)
{
#if defined(_WIN32)
    static int64_t frequency; /* fixed at boot; benign to race the init */
    if (frequency == 0)
    {
        LARGE_INTEGER freq;
        QueryPerformanceFrequency(&freq);
        frequency = freq.QuadPart;
    }
    LARGE_INTEGER counter;
    QueryPerformanceCounter(&counter);
    /* split to avoid both truncation (freq / 1e6 first) and the int64
       overflow of counter * 1e6 (~10 days of uptime at 10MHz) */
    return (counter.QuadPart / frequency) * 1000000 + (counter.QuadPart % frequency) * 1000000 / frequency;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
#endif
}

/* The writers guarantee single threaded access (see the writes_arena comment),
   so growth races only with concurrent drains holding the previous generation;
   those keep working on it safely - the retired chain outlives them all. */
static struct dirty_bitmap *
_grow_dirty_bitmap(struct handle_global_state *global_state, struct dirty_bitmap *current, int64_t max_bit)
{
    int64_t words = current != NULL ? current->number_of_words * 2 : WRITEBACK_MIN_BITMAP_WORDS;
    while (words * 64 <= max_bit)
        words *= 2;

    struct dirty_bitmap *bm = calloc(1, sizeof(struct dirty_bitmap) + (size_t)words * sizeof(uint64_t));
    if (bm == NULL)
        return current; /* tracking degrades, fdatasync still covers everything */

    bm->number_of_words = words;
    bm->prev = current;
    if (current != NULL)
    {
        /* racy against a concurrent drain clearing words - a bit consumed after
           we copied it stays set in the new generation, which is a redundant,
           idempotent push on the next drain */
        memcpy(bm->words, (void *)current->words, (size_t)current->number_of_words * sizeof(uint64_t));
    }
    rvn_atomic_store_ptr(&global_state->dirty_bitmap, bm);
    return bm;
}

/* plain read-modify-write: the only concurrent mutator is the drain's exchange,
   and every interleaving is benign (see the header comment) */
static void
_set_dirty_bits(struct dirty_bitmap *bm, int64_t start_bit, int64_t bit_count)
{
    for (int64_t bit = start_bit; bit < start_bit + bit_count;)
    {
        int64_t word = bit >> 6;
        int32_t bit_in_word = (int32_t)(bit & 63);
        int64_t bits_in_this_word = rvn_min(64 - bit_in_word, start_bit + bit_count - bit);
        uint64_t mask = bits_in_this_word == 64
                            ? ~0ULL
                            : (((1ULL << bits_in_this_word) - 1) << bit_in_word);
        bm->words[word] |= mask;
        bit += bits_in_this_word;
    }
}

PRIVATE void
_mark_dirty_pages(void *handle, struct page_to_write *buffers, int32_t count)
{
    struct handle *handle_ptr = handle;
    struct handle_global_state *global_state = handle_ptr->global_state;
    if (count <= 0 || !_should_track_dirty_ranges(global_state->open_flags))
        return;

    /* the writers require sorted buffers (their run-merging depends on it),
       so the last buffer holds the highest page */
    int64_t max_bit = ((buffers[count - 1].page_num + buffers[count - 1].count_of_pages) * VORON_PAGE_SIZE - 1) / WRITEBACK_BYTES_PER_BIT;

    struct dirty_bitmap *bm = rvn_atomic_load_ptr(&global_state->dirty_bitmap);
    if (bm == NULL || max_bit >= bm->number_of_words * 64)
        bm = _grow_dirty_bitmap(global_state, bm, max_bit);
    if (bm == NULL || max_bit >= bm->number_of_words * 64)
        return; /* allocation failed, see _grow_dirty_bitmap */

    for (int32_t i = 0; i < count; i++)
    {
        int64_t first_bit = (buffers[i].page_num * VORON_PAGE_SIZE) / WRITEBACK_BYTES_PER_BIT;
        int64_t last_bit = ((buffers[i].page_num + buffers[i].count_of_pages) * VORON_PAGE_SIZE - 1) / WRITEBACK_BYTES_PER_BIT;
        _set_dirty_bits(bm, first_bit, last_bit - first_bit + 1);
    }
}

struct writeback_ctx
{
    struct handle *handle;
    struct rvn_writeback_stats *stats;
    int64_t max_bytes;
    int32_t depth;
    bool initiate_only; /* trickle mode: start writeback, never wait on it */
    int32_t pending_count;
    int32_t pending_head;
    bool budget_exhausted;
    struct
    {
        int64_t offset;
        int64_t length;
        int64_t busy_micros; /* time already spent in the start call */
    } pending[WRITEBACK_MAX_PIPELINE_DEPTH];
};

static int32_t
_wb_complete_oldest(struct writeback_ctx *ctx, int32_t *detailed_error_code)
{
    int32_t slot = ctx->pending_head;
    int64_t before = _wb_now_micros();
    int32_t rc = _writeback_range_complete(ctx->handle, ctx->pending[slot].offset, ctx->pending[slot].length, detailed_error_code);
    int64_t range_micros = (_wb_now_micros() - before) + ctx->pending[slot].busy_micros;

    ctx->stats->total_wait_micros += range_micros;
    if (range_micros > ctx->stats->max_range_wait_micros)
        ctx->stats->max_range_wait_micros = range_micros;

    ctx->pending_head = (ctx->pending_head + 1) % WRITEBACK_MAX_PIPELINE_DEPTH;
    ctx->pending_count--;
    return rc;
}

static int32_t
_wb_emit(struct writeback_ctx *ctx, int64_t offset, int64_t length, int32_t *detailed_error_code)
{
    if (!ctx->initiate_only && ctx->pending_count == ctx->depth)
    {
        int32_t rc = _wb_complete_oldest(ctx, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
    }

    int64_t before = _wb_now_micros();
    int32_t rc = _writeback_range_start(ctx->handle, offset, length, detailed_error_code);
    if (rc != SUCCESS)
        return rc;

    if (!ctx->initiate_only)
    {
        int32_t slot = (ctx->pending_head + ctx->pending_count) % WRITEBACK_MAX_PIPELINE_DEPTH;
        ctx->pending[slot].offset = offset;
        ctx->pending[slot].length = length;
        ctx->pending[slot].busy_micros = _wb_now_micros() - before;
        ctx->pending_count++;
    }

    ctx->stats->bytes_written += length;
    ctx->stats->ranges_written++;
    if (ctx->stats->bytes_written >= ctx->max_bytes)
        ctx->budget_exhausted = true;
    return SUCCESS;
}

/* emits [start_bit, start_bit + bit_count) in block-sized pieces; on budget
   exhaustion the unpushed tail is returned to the bitmap */
static int32_t
_wb_emit_run(struct writeback_ctx *ctx, struct dirty_bitmap *bm, int64_t start_bit, int64_t bit_count,
             int64_t block_bits, int32_t *detailed_error_code)
{
    while (bit_count > 0)
    {
        if (ctx->budget_exhausted)
        {
            _set_dirty_bits(bm, start_bit, bit_count);
            return SUCCESS;
        }
        int64_t bits = rvn_min(bit_count, block_bits);
        int32_t rc = _wb_emit(ctx, start_bit * WRITEBACK_BYTES_PER_BIT, bits * WRITEBACK_BYTES_PER_BIT, detailed_error_code);
        if (rc != SUCCESS)
            return rc;
        start_bit += bits;
        bit_count -= bits;
    }
    return SUCCESS;
}

EXPORT int32_t
rvn_pager_writeback_dirty(void *handle,
                          int64_t max_bytes,
                          int32_t pipeline_depth,
                          int32_t block_size_bytes,
                          struct rvn_writeback_stats *stats,
                          int32_t *detailed_error_code)
{
    struct handle *handle_ptr = handle;
    struct handle_global_state *global_state = handle_ptr->global_state;
    memset(stats, 0, sizeof(*stats));
    *detailed_error_code = 0;

    struct dirty_bitmap *bm = rvn_atomic_load_ptr(&global_state->dirty_bitmap);
    if (bm == NULL)
        return SUCCESS; /* nothing was ever tracked for this pager */

    if (!_writeback_supported(handle_ptr))
        return FAIL_WRITEBACK_NOT_SUPPORTED;

    /* depth 0 = trickle: initiate writeback and return without ever waiting on it */
    bool initiate_only = pipeline_depth == 0;
    if (pipeline_depth < 1)
        pipeline_depth = 1;
    if (pipeline_depth > WRITEBACK_MAX_PIPELINE_DEPTH)
        pipeline_depth = WRITEBACK_MAX_PIPELINE_DEPTH;
    if (block_size_bytes < WRITEBACK_BYTES_PER_BIT)
        block_size_bytes = WRITEBACK_BYTES_PER_BIT;
    int64_t block_bits = block_size_bytes / WRITEBACK_BYTES_PER_BIT;

    struct writeback_ctx ctx = {
        .handle = handle_ptr,
        .stats = stats,
        .max_bytes = max_bytes <= 0 ? INT64_MAX : max_bytes,
        .depth = pipeline_depth,
        .initiate_only = initiate_only,
    };

    int32_t rc = SUCCESS;
    int64_t run_start_bit = -1;
    int64_t run_bits = 0;

    for (int64_t w = 0; w < bm->number_of_words && !ctx.budget_exhausted; w++)
    {
        /* plain pre-check first: most words are zero and skipping them must not
           cost a locked op; only claim a word that actually has bits */
        uint64_t bits = bm->words[w];
        if (bits != 0)
            bits = rvn_atomic_xchg64(&bm->words[w], 0);

        if (bits == 0)
        {
            if (run_start_bit >= 0)
            {
                rc = _wb_emit_run(&ctx, bm, run_start_bit, run_bits, block_bits, detailed_error_code);
                run_start_bit = -1;
                run_bits = 0;
                if (rc != SUCCESS)
                    goto done;
            }
            continue;
        }

        int64_t word_base = w * 64;

        while (bits != 0)
        {
            int32_t first = _wb_ctz64(bits);
            uint64_t shifted = bits >> first;
            /* the bit-63 sentinel keeps ctz defined for the all-ones word; the run
               then measures 63 and its last bit just extends it next iteration */
            int32_t segment = _wb_ctz64(~shifted | (1ULL << 63));
            int64_t segment_start = word_base + first;

            if (run_start_bit >= 0 && segment_start != run_start_bit + run_bits)
            {
                rc = _wb_emit_run(&ctx, bm, run_start_bit, run_bits, block_bits, detailed_error_code);
                run_start_bit = -1;
                run_bits = 0;
                if (rc != SUCCESS)
                    goto done;
            }
            if (run_start_bit < 0)
                run_start_bit = segment_start;
            run_bits += segment;

            /* emit whole blocks eagerly so the pipeline fills as we scan */
            while (run_bits >= block_bits && !ctx.budget_exhausted)
            {
                rc = _wb_emit(&ctx, run_start_bit * WRITEBACK_BYTES_PER_BIT, block_bits * WRITEBACK_BYTES_PER_BIT, detailed_error_code);
                if (rc != SUCCESS)
                    goto done;
                run_start_bit += block_bits;
                run_bits -= block_bits;
            }

            if (first + segment >= 64)
                bits = 0;
            else
                bits &= ~(((1ULL << segment) - 1) << first);

            if (ctx.budget_exhausted)
            {
                if (bits != 0)
                    bm->words[w] |= bits; /* put back what we did not process */
                break;
            }
        }
    }

    if (run_start_bit >= 0)
    {
        rc = _wb_emit_run(&ctx, bm, run_start_bit, run_bits, block_bits, detailed_error_code);
        if (rc != SUCCESS)
            goto done;
    }

done:
    while (ctx.pending_count > 0)
    {
        int32_t completion_error = 0;
        int32_t wait_rc = _wb_complete_oldest(&ctx, rc == SUCCESS ? detailed_error_code : &completion_error);
        if (wait_rc != SUCCESS && rc == SUCCESS)
            rc = wait_rc;
    }

    /* racy diagnostic count - plain loads, let it vectorize */
    for (int64_t w = 0; w < bm->number_of_words; w++)
    {
        if (bm->words[w] != 0)
            stats->set_bits_remaining += _wb_popcnt64(bm->words[w]);
    }
    return rc;
}
