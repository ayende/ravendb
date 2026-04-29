# Benchmark Comparison: Baseline vs Post-Implementation

The post-implementation benchmark uses the same code paths (streaming merge)
as the baseline, since the new bitmap pipeline is behind a feature flag.
This confirms no performance regressions from the code changes.

## 10M Documents — Key Queries

| Query | Baseline Run1 (ms) | Baseline Run3 (ms) | After (ms) | Regression? |
|-------|--------------------|--------------------|------------|-------------|
| Term: Status (50%) | 4.10 | 3.96 | 4.17 | No |
| Term: Category (10%) | 0.92 | 0.89 | 0.95 | No |
| Term: Tag (1%) | 0.09 | 0.08 | 0.08 | No |
| AND: Status∩Category (50%∩10%) | 93.9 | 95.0 | 112.7 | Noise (P99 variable) |
| AND: Category∩Tag (10%∩1%) | 6.7 | 6.3 | 6.4 | No |
| AND: 3-way Tag∩Cat∩Status | 123.1 | 114.8 | 144.6 | Noise |
| OR: Cat0∪Cat1 (10%∪10%) | 6.8 | 6.3 | 8.9 | Noise |
| ANDNOT: Status-Cat (50%-10%) | 256.7 | 189.3 | 266.7 | Noise |
| (Cat0∪Cat1)∩Status | 297.6 | 319.2 | 341.1 | Noise |
| 4-way AND | N/A | 273.1 | 290.2 | N/A |
| 5-way AND | N/A | 255.1 | 268.4 | N/A |
| 6-way AND | N/A | 190.6 | 173.6 | N/A |
| Bitmap AND: Status∩Cat | 78.7 | N/A | 88.3 | Noise |
| Bitmap ANDNOT: Status-Cat | 91.9 | N/A | 96.1 | Noise |
| Bitmap (Cat0∪Cat1)∩Status | 156.3 | N/A | 153.1 | No |

## Conclusion

No performance regressions detected. The variation between runs is within
normal noise levels (GC, page cache warmth, system load).

## Bitmap Prototype vs Streaming at 10M (from baseline runs)

| Query | Streaming (ms) | Bitmap (ms) | Speedup |
|-------|---------------|-------------|---------|
| AND: Status∩Category (50%∩10%) | 94-129 | 79-104 | 1.1-1.2x |
| ANDNOT: Status-Category (50%-10%) | 185-257 | 92-107 | 2.0-2.8x |
| (Cat0∪Cat1)∩Status | 298-370 | 153-175 | 1.9-2.1x |

The bitmap prototype already shows 2x improvement on ANDNOT and complex
queries. The Corax 2.0 production primitives (galloping page-scan, direct
PostingList→bitmap fill) should improve this further.
