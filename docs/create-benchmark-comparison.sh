#!/bin/bash
# Generate benchmark comparison between baseline and after-implementation runs.
# Usage: bash docs/create-benchmark-comparison.sh

BASELINE="docs/benchmark-baseline-run1.txt"
AFTER="docs/benchmark-after-run1.txt"
OUTPUT="docs/benchmark-comparison.md"

echo "# Benchmark Comparison: Baseline vs After Implementation" > "$OUTPUT"
echo "" >> "$OUTPUT"
echo "Generated: $(date -u +%Y-%m-%d\ %H:%M:%S) UTC" >> "$OUTPUT"
echo "" >> "$OUTPUT"

for SIZE in "10,000" "100,000" "1,000,000" "10,000,000"; do
    echo "## $SIZE Documents" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
    echo "| Feature | Baseline Avg (ms) | After Avg (ms) | Change |" >> "$OUTPUT"
    echo "|---------|-------------------|----------------|--------|" >> "$OUTPUT"

    # Extract the section for this size from both files
    grep "$SIZE DOCUMENTS" -A 100 "$BASELINE" | grep -E "^(Term|AND|OR|ANDNOT|Mixed|\(|IN|Start|Exist|Range|Tag|Region|Cat|Bitmap|All|AND )" | while read -r line; do
        NAME=$(echo "$line" | sed 's/\s\+[0-9].*//')
        BASELINE_MS=$(echo "$line" | awk '{print $NF-1}' | head -1)
        # Try to find matching line in after
        AFTER_LINE=$(grep "$SIZE DOCUMENTS" -A 100 "$AFTER" | grep "^$NAME" | head -1)
        if [ -n "$AFTER_LINE" ]; then
            AFTER_MS=$(echo "$AFTER_LINE" | awk '{print $NF-1}' | head -1)
            echo "| $NAME | $BASELINE_MS | $AFTER_MS | — |" >> "$OUTPUT"
        fi
    done

    echo "" >> "$OUTPUT"
done

echo "Comparison written to $OUTPUT"
