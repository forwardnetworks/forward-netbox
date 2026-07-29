#!/usr/bin/env bash
set -uo pipefail

artifact_dir="${1:?artifact directory is required}"
volumes="${2:-1000,5000}"
rounds="${3:-3}"
profile_output="${artifact_dir}/rounds.jsonl"
resource_output="${artifact_dir}/container-resources.tsv"
profile_log="${artifact_dir}/profile.log"
profile_status="${artifact_dir}/profile.status"

mkdir -p "${artifact_dir}"
printf 'epoch_seconds\trole\tcontainer_id\tpid\tcpu_usage_usec\tmemory_bytes\n' >"${resource_output}"

sample_container() {
  local role="$1"
  local container_name="$2"
  local container_id container_pid cgroup_path usage memory epoch
  container_id="$(docker inspect -f '{{.Id}}' "${container_name}" 2>/dev/null || true)"
  container_pid="$(docker inspect -f '{{.State.Pid}}' "${container_name}" 2>/dev/null || true)"
  if [[ -z "${container_id}" || -z "${container_pid}" || "${container_pid}" == "0" ]]; then
    return
  fi
  cgroup_path="$(awk -F: '$1 == 0 {print $3}' "/proc/${container_pid}/cgroup" 2>/dev/null || true)"
  if [[ -z "${cgroup_path}" ]]; then
    return
  fi
  usage="$(awk '$1 == "usage_usec" {print $2}' "/sys/fs/cgroup${cgroup_path}/cpu.stat" 2>/dev/null || true)"
  memory="$(sed -n '1p' "/sys/fs/cgroup${cgroup_path}/memory.current" 2>/dev/null || true)"
  epoch="$(date +%s.%N)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${epoch}" "${role}" "${container_id}" "${container_pid}" \
    "${usage:-0}" "${memory:-0}" >>"${resource_output}"
}

sample_resources() {
  while true; do
    sample_container postgres fnb-merge-postgres-1
    sample_container python fnb-merge-profiler
    sleep 0.2
  done
}

sample_resources &
sampler_pid=$!

docker compose \
  --project-name fnb-merge \
  --project-directory development \
  run --name fnb-merge-profiler --rm -T \
  --volume "${artifact_dir}:/artifacts" netbox \
  bash -lc \
  "python manage.py forward_profile_merge --volumes '${volumes}' --rounds '${rounds}' --output '/artifacts/rounds.jsonl' --i-understand-this-creates-test-data" \
  >"${profile_log}" 2>&1
profile_exit=$?

kill "${sampler_pid}" 2>/dev/null || true
wait "${sampler_pid}" 2>/dev/null || true
printf 'finished_at=%s\nexit_code=%s\n' "$(date -Iseconds)" "${profile_exit}" >"${profile_status}"
exit "${profile_exit}"
