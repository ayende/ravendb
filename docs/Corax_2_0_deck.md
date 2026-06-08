# Corax 2.0 — Deck (collaborative draft)

> **How to edit this file.** Each slide is one section. Inside a slide you'll find four blocks:
> - **Header** — title + subtitle as they appear on the slide
> - **On-slide** — the actual visible content (text, code, callouts)
> - **Design** — layout, colour treatment, panels, where things sit
> - **Speaker notes** — what you'd say out loud (also pasted into PowerPoint notes when we re-build)
>
> Strike through, rewrite, leave `TODO:` markers — anything you want.
> The goal is to nail content + structure here first, then I'll regenerate the .pptx in one pass.
>
> **Brand palette** referenced throughout: `blue #388EE9`, `turq #1CC8EE`, `mint #3DC983`,
> `mintSoft #2BA86E`, `redAcc #D9483E`, `amber #F1B96E`, `black #0F1425`,
> `codeBg #111A2E`, `codeBg2 #1A2540`, `lightGrey #F7F5FA`.
> Fonts: **Montserrat** (head + body), **JetBrains Mono** (code).

---

## Deck shape

| # | Title | Theme | Status |
|---|-------|-------|--------|
| 1 | Corax 2.0 | dark · title | done |
| 2 | The scenario · SuperUser dataset | light | done |
| 3 | The plan Corax 1.0 hands you today | light | done |
| 4 | What the type system actually looks like | light | done |
| 5 | The maintenance cost | light | done |
| 6 | How do we structure a new query system? | light | **draft (your notes)** |
| 7 | Same query text · different plans | light | **draft (your notes)** |
| 8 | What we want | light | done |
| 9 | Caching the compiled plan | light · technical | **draft (your notes)** |
| 10 | Generating code at runtime | light | done |
| 11 | From RQL text to running code | dark · progression | done |
| 12 | From template to bitmap ops | light | done |
| 13 | Where we're moving toward | light · recap | *possibly redundant with 11 — see note* |
| 14 | RoaringBitmap · four container types | light | done |

> **Open question on slide 13.** It restates the five-stage pipeline that's already on slide 11 (dark, progression).
> One of them is probably enough. Default below: **keep 13 as a "before / after + recap" closer to the bitmap dive**,
> drop the pipeline strip from it so it doesn't repeat slide 11. Flag if you want to remove 13 outright.

---

## Slide 1 — Title

**Header**
- Title: **Corax 2.0**
- Eyebrow: `RAVENDB`
- Subtitle: *Modernizing the query engine*
- Caption: *Architecture & Review Guide*
- Footer: `RAVENDB-25281 · v7.2 → v8.0 · Internal`

**On-slide**
- Big "Corax 2.0" wordmark, white on black
- Racing Rook image on the right (asset: `rook_racing_brand.png`)
- Motion streaks behind the Rook (asset: `streaks.png`, semi-transparent)
- Mint accent bar under the title
- Bottom-left: `RavenDB` brand mark + the RAVENDB-25281 line

**Design**
- Dark background (`#0F1425`)
- Left half: text stack
- Right half: full-bleed Rook image, 4.0″ × 7.5″, x=7.6, y=0
- Title size: 72pt bold, char-spacing -2
- Subtitle: 26pt turquoise
- Caption: 16pt italic grey

**Speaker notes**
- Welcome. This deck walks through Corax 2.0 — the bitmap query pipeline that replaces Corax 1.0's
  generic-struct iterator tree.
- Scope is **compute only**. We did not touch the storage engine, on-disk format, indexing API, or entry-ID semantics.
  Posting lists, compact trees, entry blobs are untouched. **No re-indexing required.**
- Plan to spend roughly two minutes per slide for a 30-minute walkthrough.
  All code references are absolute (file + section). Reviewers can pull up the PR alongside.

---

## Slide 2 — The scenario · SuperUser dataset

**Header**
- Title: **The scenario · SuperUser dataset**
- Subtitle: *1.37 M users, 477 K questions · top-15 active users with specific badges · paginated, sorted, useful.*

**On-slide**
- **Four stat tiles across the top** (dark cards on light bg):
  - **1.37 M** users
  - **477 K** questions
  - **4** badge filters
  - **15** page size
- **Big RQL block** (dark code panel, full width) showing:
  ```
  $reputation = 100
  $created    = '2010-01-01'
  $badges     = ["Teacher", "Scholar", "Revival", "Commentator"]

  from "Users"
  where Reputation > $reputation
    and CreationDate > $created
    and Badges[].Name in ($badges)
  order by Views as long desc
  include timings()  limit 15
  ```
- **Result callout** (amber-bordered) at the bottom:
  - Eyebrow: *Corax 1.0 (RavenDB 7.2.2)*
  - Big number: **~70 ms** per execution
  - Subline: *User-facing, paginated, top-N — exactly the shape that has to be fast.*

**Design**
- Light theme
- Stat tiles: 4 across, codeBg fill, big turquoise numbers (28pt), grey labels
- RQL panel: codeBg, JetBrains Mono 14pt, keywords in `codeStr` (gold), code in `codeFg` (off-white), parameter names in turquoise
- Bottom callout: `#FFF7E8` fill with `amber` 0.6pt border, mint accent stripe on the left

**Speaker notes**
- Anchor query for the whole deck. Realistic dataset (SuperUser export — 1.37 M users, 477 K questions).
- The query asks for the **top 15** active users who have ANY of four well-known badges, were created after 2010, and have reputation above 100. Sort by view count desc.
- User-facing, paginated, sorted, top-N. *This is the shape that decides whether RavenDB feels snappy.*
- Baseline on Corax 1.0 / RavenDB 7.2.2: **~70 ms per execution.** Hold onto that number — it'll reappear.

---

## Slide 3 — The plan Corax 1.0 hands you today

**Header**
- Title: **The plan Corax 1.0 hands you today**
- Subtitle: *`include timings()` returns this tree of generic-struct matches · what ran, not how or why.*

**On-slide**
- **Big SVG diagram** of the v1 query plan (rendered from DOT, restyled to brand palette).
  Top-down layout:
  - `SortingMatch (sort = Views desc)` — mint
  - `BinaryMatch [And]  count = 0 (conf: Low)` — amber-tinted (warning)
    - LEFT: `BinaryMatch [Or]  count = 237,729` — grey
      - LEFT: `BinaryMatch [Or]  count = 196,371`
        - `TermMatch [Set] Badges.Name = Teacher  count = 112,045`
        - `TermMatch [Set] Badges.Name = Scholar  count = 84,326`
      - RIGHT: `BinaryMatch [Or]  count = 41,358`
        - `TermMatch [Set] Badges.Name = Revival  count = 15,988`
        - `TermMatch [Set] Badges.Name = Commentator  count = 25,370`
    - RIGHT: `BinaryMatch [And]  count = 0 (conf: Low)` — amber-tinted
      - `MultiTermMatch  count = 295,937` → `TermNumericRangeProvider Reputation-L > 101`
      - `MultiTermMatch  count = 1,366,366` → `TermNumericRangeProvider CreationDate-L > 2010-01-01`
- **Red callout at the bottom**:
  - *"Two ANDs report `count = 0 (Confidence: Low)`. No operand order, no posting-source kind, no compiled-plan cache key, no IL. **You see WHAT ran, not HOW or WHY.**"*
- **Pull-quote stamp** (top-right or floating): *"Computed per query execution!"*

**Design**
- Light theme
- SVG fills most of the slide (~5.0″ tall, full-width)
- TermMatch leaves: blue fill, white text
- MultiTermMatch nodes: turquoise fill
- TermNumericRangeProvider leaves: blue fill, white text
- BinaryMatch[And] with low-confidence count: amber fill, dark amber border, dark amber text
- The red callout has the `redAcc` left bar + `#FFEEE9` fill + red text — it's the "this is the problem" stamp

**Speaker notes**
- This is what `include timings()` hands a user today.
- Top is `SortingMatch` wrapping a `BinaryMatch[And]`. Left half is the OR-tree over four badges; right half is the AND of the two range predicates.
- **Two AND nodes report `count = 0 (Confidence: Low)`** — that's not a diagram bug. The planner literally has no estimate of how selective the intersection is, so it picks an operand order without that information.
- Every node here is a **generic struct instantiation**: `BinaryMatch<MultiTermMatch, BinaryMatch<…, BinaryMatch<…, …>>>`. Next slide unpacks what that means.
- What's **not** here: post-sort operand order, which clause ran first and why, the posting-source kind (tree scan? posting list?), the cache key, the IL. None of it. Even the count column is recomputed every execution — which is wasted work.

---

## Slide 4 — What the type system actually looks like

**Header**
- Title: **What the type system actually looks like**
- Subtitle: *Generic-struct trampoline · type erased at every nest level · ~7 erasure files to keep it compiling.*

**On-slide**
- **Two side-by-side panels** at the top:
  - **IN THEORY** (mint header) — *What you'd write with full generics pipeline:*
    ```csharp
    DeduplicationMatch<
      BinaryMatch<MultiTermMatch,
        BinaryMatch<MultiTermMatch,
          BinaryMatch<MultiTermMatch, MultiTermMatch>
        >
      >
    > match;
    ```
  - **IN PRACTICE** (red header) — *Each nest level erases the inner type to `IQueryMatch`:*
    ```csharp
    DeduplicationMatch<BinaryMatch>
      BinaryMatch<MultiTermMatch, BinaryMatch>
        BinaryMatch<MultiTermMatch, BinaryMatch>
          BinaryMatch<MultiTermMatch, MultiTermMatch>

    // every "BinaryMatch" name above hides
    // a different concrete generic struct.
    // Only the OUTERMOST type parameter is
    // preserved at each level.
    ```
- **THE COST · 7 erasure files like this one** (red-bordered card, full width):
  ```csharp
  BinaryMatch Build<TBuildInner, TBuildOuter>(
      in TBuildInner buildInnerSet,
      in TBuildOuter buildOuterSet,
      in CancellationToken token = default)
    where TBuildInner : IQueryMatch
    where TBuildOuter : IQueryMatch
    => BinaryMatch.Create(
         BinaryMatch<TBuildInner, TBuildOuter, BinaryMatch.And>
           .YieldAnd(this, in buildInnerSet, in buildOuterSet, token));
  ```

**Design**
- Light theme
- Two top panels, mint and red headers
- Bottom: red-bordered card spanning full width, with codeBg fill for the code

**Speaker notes**
- What the SuperUser query actually compiles to.
- *In theory*: a clean nested generic. Four leaves, three `BinaryMatch` nest levels, a `DeduplicationMatch` wrapper. Six type parameters.
- *In practice*: that type doesn't fit because of how generic-struct dispatch works in C#. We erase the inner type to `IQueryMatch` at each nest level. The outer face of every `BinaryMatch` in the chain is just `BinaryMatch` — the second type parameter shows up *one level down*.
- This erasure costs ~7 files of `BinaryMatch.Build<…>`-style factory shims. Bottom panel is the canonical example.
- Every new match type — `AndNotMatch`, `BoostingMatch`, `MultiUnaryMatch`, etc. — has to thread through these factories or the dispatch silently falls back to the catch-all and the optimization disappears.
- This is the **structural** reason for the complexity tax — why adding a predicate is a half-day of plumbing rather than a one-line change.

---

## Slide 5 — The maintenance cost

**Header**
- Title: **The maintenance cost**
- Subtitle: *When the code is this opaque, even the authors lose track of what it already does.*

**On-slide**
- **Big pull-quote** card (dark fill, mint accent stripe):
  > " I spent half an hour explaining to Maciej how a particular optimization *should* work
  > — getting him thoroughly confused —
  > **because we had already implemented it.**
- **Three impact cards** below the quote:

  | Opacity | Lost knowledge | Carrying cost |
  |---------|----------------|---------------|
  | Code is dense, hard to read & track. Various behaviors are scattered in different places. **Challenge — where in Corax do we decide what clause should run first?** | Optimizations get added, used once, then forgotten. Even the author can't remember whether the path exists; the reader can't easily tell from the code what is going on. | Every new feature pays a fixed up-front tax — author it, then add it to seven erasure switches. Every refactor requires a massive effort to go through so much craft. |

**Design**
- Light theme
- Quote card: `#111A2E` fill, mint left bar, giant 80pt smart-quote `"` in mint, white text 18pt
- Three impact cards: white fill, `redAcc` top-bar accent, black titles, grey body

**Speaker notes**
- The soft cost of the type-erasure pattern. Not the **70 ms runtime cost** — the **maintenance** cost.
- Concrete example. Half an hour walking Maciej through how a particular query optimization *should* work in Corax, getting him increasingly confused, before realising **we already shipped it**.
  The implementation was sitting in the code — but the call site was buried under three layers of generic-struct erasure with conditional dispatch in the middle. The type signature gave no hint that the optimization fired.
- Three things compound:
  1. **Opacity** — you can't grep these types, you can't step through them in a debugger without hitting six identical-looking frames, and the optimization decisions are made by delegate pointers chosen at construction.
  2. **Lost knowledge** — optimizations get added in code reviews, nobody touches the file for a year, knowledge evaporates. When the author can't remember whether the path exists, you have a problem.
  3. **Carrying cost** — every new feature has to add itself to ~7 erasure switches. Every refactor risks silently dropping a path because one switch falls through to the catch-all.
- Bottom line: 70 ms is the easy number to put on a slide. The hard one is the human cost — and that's what 2.0 has to fix.

---

## Slide 6 — How do we structure a new query system?

> **Drafted from your notes.** *"Problem – how do we structure new query system? The current system (object graph + pulling) works, but either require super generic code or complex trampolines. We want to add structure to the system, so we have proper separation. (can also merge stuff from the next slide) We need to be able to easily separate the query PLAN from its EXECUTION. We need to be able to shift those as needed, rewrite it, etc. Another aspect is caching – right now, it costs a lot to compute the query plan, and it takes from the query time. But the same query text can have different plans… (leave to the next one)"*

**Header**
- Title: **How do we structure a new query system?**
- Subtitle: *Add structure. Separate plan from execution. Make both inspectable, swappable, cacheable.*

**On-slide**

Two-column layout under a thin "Goals" header strip.

**Left column — "The current system, in one diagram"** (small inline sketch + caption)
- Tiny block diagram (3 stacked boxes):
  - `IQueryMatch  ← object graph`
  - `↓ Fill(span) ← caller-driven pull`
  - `Span<long>   → next match in the chain`
- Caption: *"Works. But every node is either super-generic code OR a hand-written trampoline. Plan and execution are the same tree of generic structs — you can't reason about one without the other."*

**Right column — "What we want"** (4 numbered statements, each one line big + one line small)
1. **Separate PLAN from EXECUTION.**
   *Two distinct artifacts. The plan describes WHAT and WHY; the execution does the work.*
2. **Both representations are data.**
   *A plan is a flat array we can serialise, hash, log, swap, rewrite, A/B.*
3. **Execution is generated.**
   *We emit IL for the plan we picked. No giant generic struct, no trampoline.*
4. **Pay plan cost once. Pay execution cost every time.**
   *Cache the plan. Re-use the IL. Spend the saved time on a better plan.*

**Bottom strip** (amber-bordered, full width):
- *"Same query text can produce DIFFERENT plans depending on parameter values. Next slide."* (arrow → slide 7)

**Design**
- Light theme
- Left sketch: small (3″ wide), grey/blue boxes, simple arrows
- Right list: tight number-and-statement, blue numbers (32pt), bold black headlines, grey sublines
- Bottom strip: amber stripe + italic forward-reference

**Speaker notes**
- The motivation slide. Why we're not just optimizing 1.0 — we're rebuilding it.
- The current architecture is an **object graph** of `IQueryMatch` nodes, **pulled** by `Fill(Span<long>)` callbacks. Works, has worked for years, but every node has to be either super-generic (paying for runtime flexibility on every call) or a custom trampoline (paying for it in author-time).
- The plan and the execution are **the same tree of generic structs.** You can't change one without changing the other. You can't inspect the plan separately from running it. You can't cache the plan because there's nothing to cache — the only artifact IS the running tree.
- Three structural changes:
  1. **Separate plan from execution** as distinct artifacts.
  2. **Make both representations data** — flat arrays. Serialisable. Hashable. Editable.
  3. **Generate execution code** for the plan we picked. The expensive runtime cost moves to a one-time emit step.
- And once plans are data, plans can be **cached**. Right now we rebuild the plan on every execution; that work shows up in your query time. The cache flips that — *but* the same query text can have *different* plans depending on parameter values, which is the next slide.

---

## Slide 7 — Same query text · different plans

> **Drafted from your notes.** *"Same query text – different plans. Example: from Questions where AcceptedAnswerId != null and Tags = $tag. $tag = 'ssh' (9,857) vs. $tag = 'telnet' (417)"*

**Header**
- Title: **Same query text · different plans**
- Subtitle: *Cardinality drives operand order. Parameter values drive cardinality. Two parameter sets → two plans.*

**On-slide**
- **RQL block** at the top (one code panel, full width):
  ```
  from "Questions"
  where AcceptedAnswerId != null
    and Tags = $tag
  ```
- **Two side-by-side execution panels** sharing the same RQL above:

  **LEFT — $tag = "ssh"** *(blue card)*
  - Cardinality table:
    | clause | cardinality |
    |---|---|
    | `Tags = "ssh"` | **9,857** matches |
    | `AcceptedAnswerId != null` | ~250K matches *(estimate)* |
  - Operand order: **`Tags → AcceptedAnswerId`** *(start small, intersect away)*
  - Plan shape stamp: `digest 9CE17A33…02C8`
  - One-line cost note: *"Start with the small Tags posting list; AND-mask against AcceptedAnswerId."*

  **RIGHT — $tag = "telnet"** *(purple card)*
  - Cardinality table:
    | clause | cardinality |
    |---|---|
    | `Tags = "telnet"` | **417** matches |
    | `AcceptedAnswerId != null` | ~250K matches |
  - Operand order: **`Tags → AcceptedAnswerId`** *(also starts small — but small enough to trigger the entry-scan fast lane)*
  - Plan shape stamp: `digest 4B2901E7…F010`
  - One-line cost note: *"Tags is so small that we read entries directly instead of touching the AcceptedAnswerId posting list at all."*

- **Bottom strip** (mint-bordered, full width):
  - *"Two parameter values → two cardinality profiles → two operand orders → two cache slots, two compiled IL bodies, both held in the plan cache."*

**Design**
- Light theme
- Top RQL: codeBg, full width, code-styled
- Two cards side by side: cards have coloured header bars; tables are tight; **plan-shape stamp** is in JetBrains Mono italic, smaller, grey — communicates "this plan is identified by this digest"
- Bottom strip: `#E8F8EF` fill, mint border

**Speaker notes**
- The example: a Stack Overflow-style "find questions for tag X that have an accepted answer" query. We've all written this query.
- Plug in `$tag = "ssh"` and you get **9 857 matching tag rows**. Plug in `$tag = "telnet"` and you get **417**. Two orders of magnitude.
- The two queries are textually identical but they want different plans:
  - For `ssh`: 9 857 is small enough to seed the bitmap from but big enough to justify an `AndWith` against the `AcceptedAnswerId` posting list.
  - For `telnet`: 417 is so small that you should just read those 417 entries directly and check the `AcceptedAnswerId` field inline — no posting list walk for the second clause at all. That's the **entry-scan** fast path we'll see later.
- These are **two different `CompiledPlan` cache entries.** Same query text, same parameter *types*, different cardinality → different operand order → different SHA-256 digest → different cache slot. Both live in the plan cache simultaneously; lookup picks the right one based on the parameter values that arrive at query time.
- This is what makes plan caching nontrivial. You can't key on query text alone — you'd serve the wrong plan half the time.

---

## Slide 8 — What we want

**Header**
- Title: **What we want**
- Subtitle: *Targets going in — what 2.0 had to deliver to be worth the rewrite.*

**On-slide**
Five outcome cards across the slide, each numbered 01–05.

| # | Title | Body |
|---|-------|------|
| 01 | **Faster** | Wins on the queries that matter — paginated user-facing reads, top-N over sorted indexes, common AND chains. Not microbenchmark wins; user-visible wall-clock. |
| 02 | **Reduced complexity** | Adding a predicate, a sort path, a residual check — not a half-day of generic-struct trampoline plumbing. Less boilerplate per feature, not literally less code. |
| 03 | **Inspectable** | Plans are data. EXPLAIN tells you operand order, cardinality, cost-gate decision, what ran and what was a candidate. Same data drives execution and introspection. |
| 04 | **Runtime adaptive** | Same compiled IL serves different selectivity regimes. Cost-gate flips per call. No recompile to switch BitmapPipeline ↔ DirectScan ↔ EntryScan. |
| 05 | **Cacheable plans** | Compile once per shape, reuse forever across executions and transactions. Trade compile cost for time spent on better plans — and recoup it on every subsequent call. |

**Bottom strip**: *"The mechanism for all of them · turn plan and execution into data you can read."*

**Design**
- Light theme
- Five card columns of equal width
- Big colored number at top (01/02 blue, 03/04/05 mint)
- Title in bold black
- Mint underline accent
- Body in grey

**Speaker notes**
- Five targets we set going in. First two are the obvious ones — make it faster, make it less painful to maintain.
- The next three are the **real** win: making the engine introspectable, adaptive, and cacheable.
- On *less complexity*: we want *less boilerplate per feature*, not literally less code. Adding a predicate should be a definition + a few lines, not a half-day across seven trampoline files.
- On *adaptive*: same compiled body, but the runtime can flip strategies (bitmap path ↔ direct scan ↔ entry scan) based on what the intermediate bitmap actually looks like. We'll see this in detail later.
- On *cacheable*: plan cost is paid **once per shape**. The query that ran the smart 70 ms plan yesterday runs in 5 ms today because the plan is already JIT'd. We trade compile-time for runtime, and we recoup it on every subsequent call.
- One unifying mechanism makes all five work: stop hiding decisions behind generic dispatch, surface them as **flat data**.

---

## Slide 9 — Caching the compiled plan

> **Drafted from your notes.** *"Caching. SIMD + ushort + sha256. Random replacement after 32 distinct plans. Assume they are small."*

**Header**
- Title: **Caching the compiled plan**
- Subtitle: *Per-query-text bucket of up to 32 plans · ushort fingerprint scanned with SIMD · SHA-256 confirms the match.*

**On-slide**
Three horizontal bands, each one labelled with a number.

### 1. The cache shape (top band)
- Small diagram:
  ```
  PlanCache (per-index)
   ├─ query text 1 → PerQueryPlans  ┐
   ├─ query text 2 → PerQueryPlans  │  each bucket: up to 32 entries
   ├─ query text 3 → PerQueryPlans  │
   └─ …                             ┘
  ```
- Caption: *"Two layers. Outer dictionary keyed by **query text** holds buckets. Each bucket holds up to **32 CompiledPlan** entries — one per distinct plan shape that this query text has produced."*

### 2. The lookup (middle band, the meat)
Side-by-side panels:

**LEFT — "The shape digest"**
- Each plan has a **256-bit SHA-256 digest** over the load-bearing pieces of its shape:
  - clause count
  - per-clause `OriginalIndex` in post-sort order
  - per-parameter `ScanValueType`
  - per-clause sentinel outcomes
  - WHEN-clause survival mask
  - BETWEEN / IN sentinel marks
  - boost / cardinality-cliff flags
- We extract a **`ushort` fingerprint** (first 16 bits) — small enough to broadcast across a SIMD lane.

**RIGHT — "The SIMD scan"**
- Layout: `struct-of-arrays`. One `ushort[32]` of fingerprints, parallel to `CompiledPlan[32]`.
- Lookup:
  1. Broadcast target fingerprint to a Vec256 of ushorts (16 lanes).
  2. Compare against `_fingerprints[0..15]` → 16-bit mask via `ExtractMostSignificantBits`.
  3. `TrailingZeroCount` picks the first candidate lane.
  4. **Full SHA-256 compare** confirms the match (collisions are real on 16 bits — pre-filter only).
  5. Mismatch → mask off that bit and keep scanning.
- Two SIMD iterations cover the whole 32-slot bucket.

### 3. Eviction (bottom strip)
- *"32 slots fill → random replacement. We assume distinct plans per query text are **small in number** (<32 over the index's lifetime). If a query text starts churning more shapes than that, eviction stays cheap (`Random.Shared.Next` → overwrite) and re-emit cost is paid on the next miss."*

**Design**
- Light theme
- Three horizontal bands top-to-bottom, each with a numbered tab on the left
- Diagram in band 1: simple ASCII-style boxes, small (~3″ tall)
- Band 2: two equal cards — left is text/list, right is the SIMD pseudocode flow
- Band 3: thin amber strip with the eviction note

**Speaker notes**
- The plan cache is where the "pay compile cost once, reuse forever" promise gets cashed in.
- Shape (band 1): two-level cache. Outer dictionary by query text — that's the cheap key. Each query text owns a bucket of up to **32 CompiledPlans**, one per distinct shape we've seen for that text.
- Why 32: empirically, a single query text *usually* stabilises at one or two shapes. The selectivity model and the sentinel outcomes don't change much for similar parameter values. 32 is a comfortable ceiling.
- Lookup (band 2): each plan carries a **256-bit SHA-256 digest** of its load-bearing shape pieces — clause count, post-sort operand order, parameter type-tags, sentinel outcomes, WHEN-survival mask, boost flag. The full digest is the authoritative identity.
- For *fast* lookup we store a **16-bit fingerprint** parallel to the digest. A `Vector256<ushort>` holds 16 of those at a time, so two SIMD iterations cover the 32-slot bucket. Compare, extract MSB mask, trailing-zero-count to pick a candidate lane, then **full 256-bit confirm** to defeat the 16-bit collision rate.
- Eviction (band 3): once 32 slots are full, new plans overwrite at random (`Random.Shared.Next(32)`). We don't bother with LRU because (a) we assume a query text won't have >32 useful shapes and (b) if it does, re-emitting an evicted plan on the next miss is the right behaviour anyway — the JIT will tier-1 it back into fast code within a few invocations.

> **Open question.** *32* is the soft ceiling on shapes-per-text. Worth showing the math? E.g. *"4 selectivity regimes × 4 sentinel-collapse states × 2 boost flags = 32 — by construction, not arbitrary."* TODO: confirm with code before claiming this.

---

## Slide 10 — Generating code at runtime

**Header**
- Title: **Generating code at runtime**
- Subtitle: *`DynamicMethod` + `ILGenerator` → JIT-compiled native code, GC-tied to the index instance.*

**On-slide**
- **Top: API code panel** (full width, dark code-styled card) titled `API · System.Reflection.Emit`:
  ```csharp
  var method = new DynamicMethod(
      "Execute_" + plan.ShapeDigest.ToHex(),
      returnType: typeof(void),
      parameterTypes: new[] { typeof(CompiledQueryMatch) },
      m: typeof(CoraxIndexPersistence).Module,
      skipVisibility: true);

  var il = method.GetILGenerator(streamSize: planOpCount * 16);
  // emit one static call per PlanOp
  foreach (var op in plan.Ops)
      il.EmitCall(op.PrimitiveMethod, op.Slots);
  il.Emit(OpCodes.Ret);

  var @delegate = (Action<CompiledQueryMatch>)method.CreateDelegate(
      typeof(Action<CompiledQueryMatch>));
  ```
- **Three explainer cards** below the code:

  | JIT tier | Lifetime | Why dynamic |
  |---|---|---|
  | The DynamicMethod is JIT-compiled exactly like a static method. Tier-0 on first call (fast compile, conservative codegen). After ~30 invocations, tier-1 recompiles with inlining, BCE, vectorization — same optimizations a hand-written method would get. Hot queries reach steady state in under a millisecond's worth of executions. | DynamicMethod is reference-counted by the runtime. We hold the delegate on the CompiledPlan, which lives in the index's PlanCache. When the cache evicts an entry (or the index is closed), references drop and the GC reclaims the compiled native code. No manual disposal. | Plan shape depends on cardinality, parameter types, and operand order — none of which is known at build time. A static dispatch pays for that flexibility on every call. Emitting one specialised method per shape lets the JIT see straight-line code and inline aggressively; the cache amortises the emit cost. |

**Design**
- Light theme
- Top API card: ~2.8″ tall, full width
- Three explainer cards: equal width, each with a coloured top-bar (blue / mint / turq), black title, grey body

**Speaker notes**
- We emit IL at runtime via `DynamicMethod` + `ILGenerator`. This is .NET's runtime-codegen API — same machinery LINQ-to-Objects uses for compiled expression trees.
- *Top panel walks the API.* Pick a name (we use the plan's SHA-256 digest hex), pick a return type (`void`), pick parameter types (one, the `CompiledQueryMatch` context), pick a hosting module (the Corax index assembly), skip visibility checks so we can call internal methods, get an `ILGenerator`, emit one static call per `PlanOp`, terminate with `Ret`, materialise as a typed delegate.
- *JIT tier.* The DynamicMethod is just code from the runtime's perspective. Tier-0 on first invocation — fast compile, low optimization. After ~30 calls, **tier-1 recompiles** with the full optimizer running: inlining, bounds-check elimination, autovectorization. Hot queries reach steady state in well under a second of execution.
- *Lifetime.* The runtime reference-counts dynamic methods. We hold a reference to the delegate on the `CompiledPlan`, which lives in the per-index `PlanCache`. When the cache evicts or the index is closed, the references drop and **GC reclaims the native code page eventually**. No `IDisposable`, no manual cleanup.
- *Why dynamic.* Static dispatch through an interface or function pointer pays for runtime flexibility on every single call. Specialising one method per plan-shape lets the JIT see **straight-line code with no virtual sites**, which is what unlocks inlining and vectorisation. The cache amortises the emit + tier-up cost over thousands of subsequent invocations.

---

## Slide 11 — From RQL text to running code

**Header**
- Title: **From RQL text to running code**
- Subtitle: *Five stages. Each one takes the previous stage's output and gives the next one less to think about.*

**On-slide**
Five vertical stage cards, dark background, left-to-right arrows between them.

**Card 1 — `01 RQL` (turquoise band)**
- I/O: `user query text  →  a string`
- Body: *The query as the user wrote it. The same surface RavenDB has always exposed. Nothing in 2.0 changes here.*
- Sample code:
  ```
  from "Users"
  where Reputation > $r
    and CreationDate > $d
    and Badges[].Name in $b
  order by Views desc
  limit 15
  ```

**Card 2 — `02 AST` (blue band)**
- I/O: `string  →  parse tree`
- Body: *RavenDB's existing AST. Field references resolved, parameters located, WHERE / ORDER BY shape identified. Unchanged from 1.0 — we ride on this.*
- Sample tree:
  ```
  Query
   ├ From: Users
   ├ Where: And(
   │     Range(Reputation, $r),
   │     Range(CreationDate, $d),
   │     In(Badges.Name, $b))
   └ OrderBy: Views desc
  ```

**Card 3 — `03 Template` (turquoise band)**
- I/O: `AST  →  structural skeleton`
- Body: *Field-and-shape skeleton, no parameter values. **Cached per query text.** The expensive AST walk happens once — every subsequent execution of this text skips straight to stage 4.*
- Sample:
  ```
  ClauseTemplate {
    clauses: [
      Range(Reputation),
      Range(CreationDate),
      In(Badges.Name)
    ],
    sort: Views desc
  }
  ```

**Card 4 — `04 Execution` (mint band)**
- I/O: `template + params  →  a concrete plan`
- Body: *Parameter values bound, cardinality estimated, operand order chosen (cheapest clause first). **Different params → different operand order → different plan shape → different cache slot.***
- Sample:
  ```
  exec.params = [
    Reputation=100,
    CreationDate=2010,
    Badges=["Teacher",…]
  ]

  exec.order  = [
    Badges, Rep, Date
  ]
  ```

**Card 5 — `05 Emit` (deeper mint band)**
- I/O: `plan shape  →  DynamicMethod`
- Body: *Generate IL for THIS plan shape. Helpers in `QueryPrimitives` carry the structural work; the emitted code mostly threads `ctx + slot indices` into static calls.*
- Sample:
  ```
  Ctx.FillFromTreeScan
     (ctx, 0, slot=0)
  Ctx.AndWithTreeScan
     (ctx, 1, 0)
  Ctx.AndWithTreeScan
     (ctx, 2, 0)
  Ctx.SortAndLimit
     (ctx, Views, 15)
  ```

**Bottom strip**: dark navy band
- *"TWO CACHE LAYERS · `ClauseTemplate` keyed by query text · `CompiledPlan` keyed by 256-bit SHA-256 of plan shape · steady-state hits skip stages 1, 2, 5."*

**Design**
- **Dark theme** (`#0F1425` slide background)
- Five equal-width cards, each ~2″ wide × ~4.6″ tall
- Each card: dark navy fill (`#111A2E`), thin coloured top-band, light text
- Card has 4 zones top-to-bottom: tag/name → I/O sub-line → body description → inline code panel
- Grey arrows between cards
- Subtle mint accent stripe under the title

**Speaker notes**
- The full pipeline on one slide. Five stages, each takes the previous stage's output and produces something smaller / more specific.
- **01 RQL** — the user's query text. Nothing changes here. The RQL surface, the parser entry point, all unchanged from 1.0.
- **02 AST** — RavenDB's existing AST. Field references resolved, parameters located, WHERE / ORDER BY identified. We deliberately ride on the existing parser; no reason to touch it.
- **03 Template** — the **structural skeleton** of the query. Fields, clause types, sort shape, parameter slots. **No parameter values yet.** This is the artifact we cache per query text. The expensive AST walk happens once for each distinct query text.
- **04 Execution** — bind parameter values, estimate cardinality, choose operand order. Different parameter values produce different operand orders, which produce different **plan shapes**. This is where one query text turns into multiple cache slots.
- **05 Emit** — generate IL for this specific plan shape. We've made this small by lifting the structural work into helper methods on `QueryPrimitives` — the emitted code mostly threads `(ctx, slot indices)` into static calls.
- Bottom strip names the two cache layers. **ClauseTemplate** is keyed by query text. **CompiledPlan** is keyed by a 256-bit SHA-256 of the plan shape. **Steady-state hits skip stages 1, 2, and 5** — they only run stages 3 (resolve params) and 4 (look up & invoke).

---

## Slide 12 — From template to bitmap ops

**Header**
- Title: **From template to bitmap ops**
- Subtitle: *Cardinality decides clause order · 17 PlanOp kinds · one query, multiple bitmap slots.*

**On-slide**
Three numbered sections.

### STEP 1 — pick the clause order *(blue banner)*
*Per-clause cardinality estimates (from index statistics + parameter values):*

| Clause | Cardinality | Notes |
|---|---|---|
| `Badges[].Name in $badges` | **~237 K** | 4 terms · uses tree scan |
| `Reputation > $r` | **~296 K** | numeric range tree scan |
| `CreationDate > $d` | **~1.37 M** | numeric range tree scan |

Winner row (highlighted): **`Badges → Reputation → CreationDate`** (cheapest first)

### STEP 2 — emit `PlanOp[]` *(mint banner)*
```
[0]  FillFromTreeScan         Badges in $b         → bm 0
[1]  AndWithTreeScan          Reputation > $r       in/out bm 0
[2]  AndWithTreeScan          CreationDate > $d     in/out bm 0
[3]  CheckAndMaybeEntryScan   gate                  bm 0
[4]  SortAndLimit             Views desc, limit 15

// metadata on the array
plan.Slots   = 1
plan.OrderBy = [Views desc]
```

### STEP 3 — bitmap slots *(turq banner)*
*"A query holds 1 to 3 RoaringBitmap accumulators."*

Three slot cards across:

| `bm 0` — **accumulator** | `bm 1` — **scratch (OR sub-tree)** | `bm 2` — **second scratch** |
|---|---|---|
| Holds the running result. Set by the first `Fill` op, mutated in-place by each subsequent `And`/`AndNot`/`Or` against it. | When an OR group lives inside an AND chain, the OR is built up here and then folded back into the accumulator at the end. | Needed only when an AND group sits inside an OR sub-tree. PR15 (dest-addressing) replaced the older `SwapBitmaps` op with explicit slot routing. |

**Design**
- Light theme
- Three numbered horizontal panels stacked
- Step 1: cardinality table on the left, winner row highlighted with mint border
- Step 2: full-width code box, mono font, ops in mint/turq, parameter values in `codeStr` (gold)
- Step 3: three slot cards side by side, each with a coloured left bar (mint / turq / grey)

**Speaker notes**
- Concrete walk-through of how the SuperUser query becomes `PlanOp[]`.
- **Step 1 — clause order.** The cardinality estimates come from index statistics + the parameter values we just got. Badges/IN gives ~237K (sum of the four badge tags), Reputation > 100 gives ~296K, CreationDate > 2010 gives ~1.37M (effectively the whole index). **Sort cheapest first** — Badges → Reputation → CreationDate. The smallest set seeds the bitmap; subsequent ANDs only shrink it.
- **Step 2 — the plan.** Five PlanOps. First op fills the bitmap from the Badges tree-scan. Two more ops AND the range predicates into the same accumulator. `CheckAndMaybeEntryScan` is the cost-gate that decides at runtime whether to keep going on the bitmap path or switch to reading entries directly (slide 14 area material). Finally `SortAndLimit` does the top-15 by Views.
- Metadata on the array: this plan uses **1 bitmap slot** (no OR sub-tree, no mixed AND-in-OR), and it specifies the ORDER BY shape.
- **Step 3 — bitmap slots.** A query holds 1-3 RoaringBitmap accumulators. `bm 0` is always the result. `bm 1` and `bm 2` are scratch space when you have nested AND/OR groups that need to be built separately before folding back in.
- PR15 (dest-addressing) replaced the older `SwapBitmaps` op with explicit slot-routing on each op — every op now names the slot it reads from and the slot it writes to.

---

## Slide 13 — Where we're moving toward *(possibly redundant — see top of doc)*

> **Question:** this slide and slide 11 both end up showing the five-stage pipeline.
> Default treatment below: **drop the pipeline strip from this slide** and keep only the BEFORE/AFTER comparison + the cache key strip. That makes it a recap right before we dive into RoaringBitmap.
> If you want to remove the slide entirely, strike through this whole section.

**Header**
- Title: **Where we're moving toward**
- Subtitle: *Plan becomes data · execution becomes a small flat IL function · intermediate state becomes a bitmap.*

**On-slide (proposed — minus the pipeline strip)**

**Top: BEFORE / AFTER comparison** (two side-by-side cards, full width)

| **CORAX 1.0 · tree of generic structs** | **CORAX 2.0 · data + a small compiled function** |
|---|---|
| **Plan** = nested generic struct (lives in the type system). | **Plan** = flat `PlanOp[]` array — data, not types. |
| **Exec** = trampoline of delegate-pointer `Fill()` calls. | **Exec** = a single `DynamicMethod` IL delegate, compiled per query shape. |
| **State** = streaming `Span<long>` per match node, materialised on demand. | **State** = a RoaringBitmap accumulator — set ops in `O(n/64)`, `O(1)` Contains. |
| **Sort** = materialise everything, heap-sort, drop the tail. | **Sort** = walk index in sort order, intersect with bitmap, stop at LIMIT. |

**Bottom: cache key strip** (dark codeBg, full width)
- *"Plan cache · `ClauseTemplate` keyed by query text · `CompiledPlan` keyed by a 256-bit SHA-256 digest · steady-state hits skip Parse / Emit / Compile."*

**Design**
- Light theme
- Two equal cards top, grey header for 1.0, mint header for 2.0
- Each card: 4 paragraph rows aligned across the two columns so the contrast is immediate
- Bottom strip: thin dark band, code-styled

**Speaker notes**
- A recap slide before we dive into the bitmap. Four axes of change side by side.
- **Plan.** Was a nested generic struct that lived in the type system — can't be cached, can't be inspected, can't be modified. Becomes a flat `PlanOp[]` array — data we can serialise, hash, log, A/B, rewrite.
- **Execution.** Was a trampoline of delegate-pointer `Fill()` calls walking a tree of generic structs. Becomes a single `DynamicMethod` IL delegate, JIT-compiled once per plan shape and cached.
- **Intermediate state.** Was a streaming `Span<long>` per match node, materialised on demand. Becomes a RoaringBitmap accumulator — set ops in `O(n/64)` (SIMD), Contains in `O(1)` (direct bit test).
- **Sort.** Was materialise everything → heap-sort → drop the tail (`O(N log N)` on the *full* result set). Becomes a streaming walk of the sort field's index, intersecting with the bitmap, stopping at LIMIT.
- And the cache key strip names how it all gets reused — `ClauseTemplate` per query text, `CompiledPlan` per shape digest.

---

## Slide 14 — RoaringBitmap · four container types

**Header**
- Title: **RoaringBitmap · four container types**
- Subtitle: *Cardinality decides shape. Set ops are SIMD-accelerated. Containers are stolen, not cloned.*

**On-slide**
Four container cards across the slide.

| Card | Range | Storage | Visual | Notes |
|---|---|---|---|---|
| **Array** *(blue)* | `0 → 4 096 values` | `ushort[]` | sparse dots | Sorted ushort array. Sparse containers. Binary search for Contains. ~8 KB max. |
| **ArrayUnsorted** *(turq)* | appended, awaiting sort | `ushort[]` | scattered dots | Values pushed in batch (e.g. lazy OR accumulation). `Finalize()` sorts before reads. |
| **Bitmap** *(mint)* | `4 097 → 65 536` | `ulong[1024]` | grid of filled cells | Dense bitset. `O(1)` Contains. SIMD AND/OR on 64-bit words. 8 KB fixed. |
| **Range** *(red)* | contiguous span | `[start, end)` | solid bar | Special container for `id > 1000 AND id < 2000`. Zero storage beyond two ints. |

**Bottom strip** (dark fill, full width):
- *"**Free-list allocator** recycles container storage. Steady-state pipeline allocates **ZERO** containers. Set ops steal containers from the RHS rather than clone."*

**Design**
- Light theme
- Four equal cards, each with a coloured header bar
- Each card: header band → range/storage row → small visual icon → description
- Bottom: black strip with mint emphasis on "Free-list allocator" and "ZERO"

**Speaker notes**
- RoaringBitmap. Four container types, chosen at runtime by **cardinality** of the contained set.
- **Array**: 0-4096 values. Sorted `ushort[]`. Binary search for Contains. Good for sparse data — at the upper bound it's ~8 KB max.
- **ArrayUnsorted**: same storage shape but values are appended without sort discipline. Used when you're building a bitmap lazily — e.g. an OR accumulation that takes multiple passes. `Finalize()` sorts before any read.
- **Bitmap**: 4 097 → 65 536. Dense `ulong[1024]` = 8 KB fixed. `O(1)` Contains. SIMD AND/OR/XOR on 64-bit words — this is where the set-ops-per-cycle wins come from.
- **Range**: special container for contiguous ID spans. Two ints (`start`, `end`). Common for `id > X AND id < Y` after intersection collapses to a contiguous span.
- **Free-list allocator**: containers come from a per-tx free list, not from the GC. Steady-state pipeline allocates **zero** containers per query. Set ops *steal* the underlying buffer from the right-hand operand rather than cloning. The hot loop is GC-free.

---

## Appendix · what's still TBD after this draft

- Slide 9 — confirm the *32* shape ceiling math or remove the comment that claims it's structural.
- Slide 13 — decide: drop entirely, drop pipeline strip only, or keep both 11 and 13 with deliberate distinct framing.
- Slides past 14 (caching deep-dive, adaptive execution, EXPLAIN, catalog, outcomes) are in the 78-slide v2 deck — we'll bring them in section-by-section once the opening sticks.
- *Catalog (44 query variants)* and *outcomes act* are large enough to deserve their own collaborative markdown when we get there.
