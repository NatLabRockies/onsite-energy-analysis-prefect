#!/usr/bin/env bash
set -euo pipefail

PREFECT_WORK_POOL="${PREFECT_WORK_POOL:-julia-pool}"
FLOW_WORKER_NAME="${PREFECT_FLOW_WORKER_NAME:-$(hostname)-flow}"
TASK_WORKER_NAME="${PREFECT_TASK_WORKER_NAME:-$(hostname)-task}"

flow_pid=""
task_pid=""

shutdown() {
  set +e
  if [[ -n "${flow_pid}" ]]; then
    kill -TERM "${flow_pid}" 2>/dev/null || true
  fi
  if [[ -n "${task_pid}" ]]; then
    kill -TERM "${task_pid}" 2>/dev/null || true
  fi
  wait || true
}

trap shutdown INT TERM

prefect worker start \
  --pool "${PREFECT_WORK_POOL}" \
  --type process \
  --name "${FLOW_WORKER_NAME}" &
flow_pid=$!

PREFECT_TASK_WORKER_NAME="${TASK_WORKER_NAME}" python -m onsite_prefect.task_worker_main &
task_pid=$!

wait -n "${flow_pid}" "${task_pid}"
status=$?
shutdown
exit "${status}"
