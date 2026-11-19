#!/usr/bin/env bash
# Wait for the Argo Rollout to settle and report how it ended.
#   wait_for_rollout.sh <strategy> <timeout-seconds>
#
# canary:     waits for Healthy (all steps and analyses passed) or Degraded (aborted).
# blue-green: waits for the pre-promotion smoke test to pass and the rollout to pause
#             at the cutover, promotes it, then waits for post-promotion analysis.
set -euo pipefail

strategy="$1"
timeout="${2:-2700}"
namespace="mlops"
name="inference"

phase() {
  kubectl -n "$namespace" get rollout "$name" -o jsonpath='{.status.phase}'
}

wait_until() {
  local wanted="$1" deadline=$((SECONDS + timeout)) current
  while (( SECONDS < deadline )); do
    current="$(phase)"
    echo "rollout phase: ${current:-unknown}"
    case "$current" in
      Degraded)
        kubectl argo rollouts get rollout "$name" -n "$namespace" || true
        echo "Rollout degraded: an analysis failed and Argo aborted it" >&2
        return 1 ;;
      "$wanted")
        return 0 ;;
    esac
    sleep 20
  done
  echo "Timed out waiting for $wanted" >&2
  return 1
}

if [[ "$strategy" == "blue-green" ]]; then
  # Paused = green is up and the smoke test passed; waiting for the switch
  wait_until Paused
  kubectl argo rollouts promote "$name" -n "$namespace"
fi

wait_until Healthy
kubectl argo rollouts get rollout "$name" -n "$namespace"
