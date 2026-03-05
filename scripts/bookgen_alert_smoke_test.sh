#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
BookGen alert smoke test:
  1) create a controlled failed bookgen-* job
  2) verify BookGenJobFailedRecent appears in Prometheus
  3) optionally inspect Alertmanager receiver mapping
  4) cleanup test job

Optional:
  --namespace <ns>               (default: sideline-wire-dailycast)
  --monitoring-namespace <ns>    (default: log-anomaly)
  --alert-timeout-seconds <int>  (default: 600)
  --no-alertmanager-check         skip Alertmanager receiver inspection
  --keep-job                      do not delete test job
EOF
}

NAMESPACE="sideline-wire-dailycast"
MON_NS="log-anomaly"
ALERT_TIMEOUT_SECONDS="600"
CHECK_ALERTMANAGER="true"
KEEP_JOB="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --monitoring-namespace) MON_NS="$2"; shift 2 ;;
    --alert-timeout-seconds) ALERT_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --no-alertmanager-check) CHECK_ALERTMANAGER="false"; shift 1 ;;
    --keep-job) KEEP_JOB="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

JOB_NAME="bookgen-alert-smoke-$(date -u +%s)"

cleanup() {
  if [[ "${KEEP_JOB}" != "true" ]]; then
    kubectl -n "${NAMESPACE}" delete job "${JOB_NAME}" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: fail
          image: busybox
          command: ["/bin/sh", "-lc", "exit 1"]
EOF

echo "Created test job: ${JOB_NAME}"

# Wait up to 2 minutes for the job to move to failed.
for _ in $(seq 1 40); do
  failed="$(kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
  failed="${failed:-0}"
  if [[ "${failed}" =~ ^[0-9]+$ ]] && [[ "${failed}" -gt 0 ]]; then
    echo "Job failure confirmed."
    break
  fi
  sleep 3
done

if [[ "${failed:-0}" -eq 0 ]]; then
  echo "ERROR: test job did not fail in expected window."
  exit 1
fi

kubectl -n "${MON_NS}" port-forward svc/monitoring-kube-prometheus-prometheus 19090:9090 >/tmp/bookgen-alert-prom-pf.log 2>&1 &
PROM_PF_PID=$!
trap 'kill ${PROM_PF_PID} >/dev/null 2>&1 || true; cleanup' EXIT
for _ in $(seq 1 20); do
  (echo >/dev/tcp/127.0.0.1/19090) >/dev/null 2>&1 && break
  sleep 0.5
done

seen="false"
started_epoch="$(date +%s)"
while true; do
  check_out="$(
    python3 - "${JOB_NAME}" <<'PY'
import json,urllib.request,sys
job_name=sys.argv[1]
url='http://127.0.0.1:19090/api/v1/alerts'
with urllib.request.urlopen(url, timeout=5) as r:
    obj=json.loads(r.read().decode())
alerts=obj.get('data',{}).get('alerts',[])
matches=[a for a in alerts if a.get('labels',{}).get('alertname')=='BookGenJobFailedRecent' and a.get('labels',{}).get('job_name')==job_name and a.get('state')=='firing']
print(len(matches))
PY
2>/dev/null || echo "0"
  )"
  if [[ "${check_out}" =~ ^[0-9]+$ ]] && [[ "${check_out}" -gt 0 ]]; then
    seen="true"
    echo "Prometheus alert observed: BookGenJobFailedRecent for ${JOB_NAME}"
    break
  fi
  now="$(date +%s)"
  elapsed=$(( now - started_epoch ))
  if [[ "${elapsed}" -ge "${ALERT_TIMEOUT_SECONDS}" ]]; then
    break
  fi
  sleep 10
done

if [[ "${seen}" != "true" ]]; then
  echo "ERROR: BookGenJobFailedRecent not observed within ${ALERT_TIMEOUT_SECONDS}s."
  exit 1
fi

if [[ "${CHECK_ALERTMANAGER}" == "true" ]]; then
  kubectl -n "${MON_NS}" port-forward svc/monitoring-kube-prometheus-alertmanager 19093:9093 >/tmp/bookgen-alert-am-pf.log 2>&1 &
  AM_PF_PID=$!
  trap 'kill ${AM_PF_PID} >/dev/null 2>&1 || true; kill ${PROM_PF_PID} >/dev/null 2>&1 || true; cleanup' EXIT
  for _ in $(seq 1 20); do
    (echo >/dev/tcp/127.0.0.1/19093) >/dev/null 2>&1 && break
    sleep 0.5
  done
  python3 - "${JOB_NAME}" <<'PY'
import json,urllib.request,sys
job_name=sys.argv[1]
url='http://127.0.0.1:19093/api/v2/alerts'
with urllib.request.urlopen(url, timeout=5) as r:
    alerts=json.loads(r.read().decode())
matches=[a for a in alerts if a.get('labels',{}).get('alertname')=='BookGenJobFailedRecent' and a.get('labels',{}).get('job_name')==job_name]
if not matches:
    print("Alertmanager entry not yet visible for job-specific alert.")
else:
    rec=",".join(x.get('name','') for x in matches[0].get('receivers',[]))
    print(f"Alertmanager receivers for {job_name}: {rec}")
PY
fi

echo "Smoke test completed."
