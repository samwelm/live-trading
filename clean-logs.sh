#!/bin/bash
set -euo pipefail

# Patterns to filter out (reduce noise)
patterns=(
  "Last pivot"
  "HybridRenkoEngine"
  "triggers new brick"
  "Fetch window"
  " S5:"
  " candles processed"
  "get_candles_df"
  "Starting data update"
  "CandleMaintenance"
  "updated successfully"
)

files=(
  logs/trading_events.log
  logs/main.log
  logs/data_operations.log
  logs/live_trading.log
)

# Clean logs while preserving the file inode
# This ensures the logging process can continue writing
for file in "${files[@]}"; do
  if [ -f "$file" ]; then
    echo "Cleaning $file..."

    # Build sed pattern
    sed_pattern=""
    for pattern in "${patterns[@]}"; do
      if [ -z "$sed_pattern" ]; then
        sed_pattern="/$pattern/d"
      else
        sed_pattern="$sed_pattern; /$pattern/d"
      fi
    done

    # Create temp file, filter, then overwrite original (preserves inode)
    sed "$sed_pattern" "$file" > "${file}.tmp"
    cat "${file}.tmp" > "$file"
    rm "${file}.tmp"

    echo "  ✓ Cleaned $(wc -l < "$file") lines remaining"
  else
    echo "  ⚠ $file not found, skipping"
  fi
done

echo ""
echo "✅ Log cleaning complete!"
echo "Note: This preserves file inodes so logging processes continue working."
