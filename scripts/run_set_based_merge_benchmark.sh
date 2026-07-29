#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "usage: $0 CONTAINER ARTIFACT_DIR [NAMESPACE_OFFSET]" >&2
  exit 64
fi

container=$1
artifact_dir=${2%/}
namespace_offset=${3:-0}
rtk mkdir -p "$artifact_dir"

inputs=()
completed=0
for volume in 1000 5000; do
  for round in 1 2 3; do
    if (( round % 2 == 0 )); then
      engines=(set_based current)
    else
      engines=(current set_based)
    fi
    for engine in "${engines[@]}"; do
      output="$artifact_dir/mac-merge-${volume}-${engine}-round-${round}.json"
      container_output="/source/$output"
      inputs+=("$output")
      if [[ ! -s "$output" ]]; then
        rtk docker exec "$container" \
          python /source/scripts/benchmark_set_based_merge_mac.py measure \
          --engine "$engine" \
          --round "$round" \
          --rows "$volume" \
          --namespace-offset "$namespace_offset" \
          --output "$container_output" \
          --confirm-disposable-database fnb-setmerge-wpa-disposable
      fi
      completed=$((completed + 1))
      printf 'completed=%s\nvolume=%s\nround=%s\nengine=%s\n' \
        "$completed" "$volume" "$round" "$engine" \
        >"$artifact_dir/checkpoint"
      rtk sync "$output" "$artifact_dir/checkpoint"
    done
  done
done

rtk docker exec "$container" \
  python /source/scripts/benchmark_set_based_merge_mac.py aggregate \
  --inputs "${inputs[@]/#//source/}" \
  --output "/source/$artifact_dir/summary.json"
rtk sync "$artifact_dir/summary.json"
