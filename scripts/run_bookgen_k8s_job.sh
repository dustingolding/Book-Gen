#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Submit a one-shot Kubernetes Job for BookGen.

Required:
  --project-id <id>
  --bookspec-key <object-store-key>

Optional:
  --run-date <YYYY-MM-DD>                  (default: UTC today)
  --job-name <name>                        (default: bookgen-<project-id>-<unix-ts>)
  --namespace <ns>                         (default: sideline-wire-dailycast)
  --image <repo:tag>                       (default: ghcr.io/dustingolding/slw-dailycast-base:latest)
  --service-account <name>                 (default: auto-detect in namespace)
  --secret-name <name>                     (default: slw-dailycast-secrets)
  --configmap-name <name>                  (default: auto-detect in namespace)
  --bookgen-use-llm <true|false>           (default: true)
  --bookgen-llm-chapter-limit <int>        (default: 0)
  --bookgen-eval-use-llm <true|false>      (default: false)
  --bookgen-rewrite-use-llm <true|false>   (default: false)
  --bookgen-allow-lock-override <true|false> (default: false)
  --bookgen-force-redraft <true|false>     (default: false)
  --with-analytics-report <true|false>     (default: false)
  --lakefs-enabled <true|false>            (default: inherited from cluster env)
  --llm-timeout-seconds <int>              (default: 60)
  --llm-max-retries <int>                  (default: 2)
  --resource-profile <light|standard|heavy>(default: standard)
  --cpu-request <value>                    (override profile default)
  --cpu-limit <value>                      (override profile default)
  --memory-request <value>                 (override profile default)
  --memory-limit <value>                   (override profile default)
  --wait-timeout-seconds <int>             (default: 43200)
  --wait                                   wait for completion

Notes:
  - This job explicitly avoids control-plane nodes via nodeAffinity.
  - If your cluster has only a control-plane node, the job will remain Pending.
EOF
}

PROJECT_ID=""
BOOKSPEC_KEY=""
RUN_DATE="$(date -u +%F)"
NAMESPACE="sideline-wire-dailycast"
IMAGE="ghcr.io/dustingolding/slw-dailycast-base:latest"
SERVICE_ACCOUNT=""
SECRET_NAME="slw-dailycast-secrets"
CONFIGMAP_NAME=""
BOOKGEN_USE_LLM="true"
BOOKGEN_LLM_CHAPTER_LIMIT="0"
BOOKGEN_EVAL_USE_LLM="false"
BOOKGEN_REWRITE_USE_LLM="false"
BOOKGEN_ALLOW_LOCK_OVERRIDE="false"
BOOKGEN_FORCE_REDRAFT="false"
WITH_ANALYTICS_REPORT="false"
LAKEFS_ENABLED_OVERRIDE=""
LLM_TIMEOUT_SECONDS="60"
LLM_MAX_RETRIES="2"
CPU_REQUEST="500m"
CPU_LIMIT="2"
MEM_REQUEST="1Gi"
MEM_LIMIT="4Gi"
RESOURCE_PROFILE="standard"
WAIT_MODE="false"
WAIT_TIMEOUT_SECONDS="43200"
JOB_NAME=""
CPU_REQUEST_EXPLICIT="false"
CPU_LIMIT_EXPLICIT="false"
MEM_REQUEST_EXPLICIT="false"
MEM_LIMIT_EXPLICIT="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-id) PROJECT_ID="$2"; shift 2 ;;
    --bookspec-key) BOOKSPEC_KEY="$2"; shift 2 ;;
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --job-name) JOB_NAME="$2"; shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --service-account) SERVICE_ACCOUNT="$2"; shift 2 ;;
    --secret-name) SECRET_NAME="$2"; shift 2 ;;
    --configmap-name) CONFIGMAP_NAME="$2"; shift 2 ;;
    --bookgen-use-llm) BOOKGEN_USE_LLM="$2"; shift 2 ;;
    --bookgen-llm-chapter-limit) BOOKGEN_LLM_CHAPTER_LIMIT="$2"; shift 2 ;;
    --bookgen-eval-use-llm) BOOKGEN_EVAL_USE_LLM="$2"; shift 2 ;;
    --bookgen-rewrite-use-llm) BOOKGEN_REWRITE_USE_LLM="$2"; shift 2 ;;
    --bookgen-allow-lock-override) BOOKGEN_ALLOW_LOCK_OVERRIDE="$2"; shift 2 ;;
    --bookgen-force-redraft) BOOKGEN_FORCE_REDRAFT="$2"; shift 2 ;;
    --with-analytics-report) WITH_ANALYTICS_REPORT="$2"; shift 2 ;;
    --lakefs-enabled) LAKEFS_ENABLED_OVERRIDE="$2"; shift 2 ;;
    --llm-timeout-seconds) LLM_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --llm-max-retries) LLM_MAX_RETRIES="$2"; shift 2 ;;
    --resource-profile) RESOURCE_PROFILE="$2"; shift 2 ;;
    --cpu-request) CPU_REQUEST="$2"; CPU_REQUEST_EXPLICIT="true"; shift 2 ;;
    --cpu-limit) CPU_LIMIT="$2"; CPU_LIMIT_EXPLICIT="true"; shift 2 ;;
    --memory-request) MEM_REQUEST="$2"; MEM_REQUEST_EXPLICIT="true"; shift 2 ;;
    --memory-limit) MEM_LIMIT="$2"; MEM_LIMIT_EXPLICIT="true"; shift 2 ;;
    --wait-timeout-seconds) WAIT_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --wait) WAIT_MODE="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

apply_resource_profile_defaults() {
  case "${RESOURCE_PROFILE}" in
    light)
      [[ "${CPU_REQUEST_EXPLICIT}" == "false" ]] && CPU_REQUEST="250m"
      [[ "${CPU_LIMIT_EXPLICIT}" == "false" ]] && CPU_LIMIT="1"
      [[ "${MEM_REQUEST_EXPLICIT}" == "false" ]] && MEM_REQUEST="512Mi"
      [[ "${MEM_LIMIT_EXPLICIT}" == "false" ]] && MEM_LIMIT="2Gi"
      ;;
    standard)
      [[ "${CPU_REQUEST_EXPLICIT}" == "false" ]] && CPU_REQUEST="500m"
      [[ "${CPU_LIMIT_EXPLICIT}" == "false" ]] && CPU_LIMIT="2"
      [[ "${MEM_REQUEST_EXPLICIT}" == "false" ]] && MEM_REQUEST="1Gi"
      [[ "${MEM_LIMIT_EXPLICIT}" == "false" ]] && MEM_LIMIT="4Gi"
      ;;
    heavy)
      [[ "${CPU_REQUEST_EXPLICIT}" == "false" ]] && CPU_REQUEST="1000m"
      [[ "${CPU_LIMIT_EXPLICIT}" == "false" ]] && CPU_LIMIT="4"
      [[ "${MEM_REQUEST_EXPLICIT}" == "false" ]] && MEM_REQUEST="2Gi"
      [[ "${MEM_LIMIT_EXPLICIT}" == "false" ]] && MEM_LIMIT="8Gi"
      ;;
    *)
      echo "Invalid --resource-profile: ${RESOURCE_PROFILE}. Use light, standard, or heavy."
      exit 1
      ;;
  esac
}

if [[ -z "${PROJECT_ID}" || -z "${BOOKSPEC_KEY}" ]]; then
  echo "--project-id and --bookspec-key are required."
  usage
  exit 1
fi

apply_resource_profile_defaults

if [[ "${WITH_ANALYTICS_REPORT}" != "true" && "${WITH_ANALYTICS_REPORT}" != "false" ]]; then
  echo "--with-analytics-report must be true or false."
  exit 1
fi
if [[ "${BOOKGEN_ALLOW_LOCK_OVERRIDE}" != "true" && "${BOOKGEN_ALLOW_LOCK_OVERRIDE}" != "false" ]]; then
  echo "--bookgen-allow-lock-override must be true or false."
  exit 1
fi
if [[ "${BOOKGEN_FORCE_REDRAFT}" != "true" && "${BOOKGEN_FORCE_REDRAFT}" != "false" ]]; then
  echo "--bookgen-force-redraft must be true or false."
  exit 1
fi
if [[ -n "${LAKEFS_ENABLED_OVERRIDE}" ]] && [[ "${LAKEFS_ENABLED_OVERRIDE}" != "true" && "${LAKEFS_ENABLED_OVERRIDE}" != "false" ]]; then
  echo "--lakefs-enabled must be true or false."
  exit 1
fi

pick_existing() {
  local kind="$1"
  shift
  for candidate in "$@"; do
    if [[ -z "${candidate}" ]]; then
      continue
    fi
    if kubectl -n "${NAMESPACE}" get "${kind}" "${candidate}" >/dev/null 2>&1; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

if [[ -z "${SERVICE_ACCOUNT}" ]]; then
  SERVICE_ACCOUNT="$(
    pick_existing serviceaccount \
      "slw-dailycast-slw-dailycast" \
      "slw-bookgen-bookgen" \
      "slw-dailycast" \
      "default"
  )"
fi

if [[ -z "${CONFIGMAP_NAME}" ]]; then
  CONFIGMAP_NAME="$(
    pick_existing configmap \
      "slw-dailycast-slw-dailycast-config" \
      "slw-bookgen-bookgen-config" \
      "slw-dailycast-config"
  )"
fi

if ! kubectl -n "${NAMESPACE}" get secret "${SECRET_NAME}" >/dev/null 2>&1; then
  SECRET_NAME="$(
    pick_existing secret \
      "slw-dailycast-secrets"
  )"
fi

if [[ -z "${SERVICE_ACCOUNT}" || -z "${CONFIGMAP_NAME}" || -z "${SECRET_NAME}" ]]; then
  echo "Could not resolve required k8s resources in namespace ${NAMESPACE}."
  echo "Resolved values: serviceAccount='${SERVICE_ACCOUNT}', configMap='${CONFIGMAP_NAME}', secret='${SECRET_NAME}'"
  exit 1
fi

if [[ -z "${JOB_NAME}" ]]; then
  SAFE_PROJECT_ID="$(printf '%s' "${PROJECT_ID}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9-' '-' | sed 's/^-*//; s/-*$//')"
  JOB_NAME="bookgen-${SAFE_PROJECT_ID}-$(date -u +%s)"
fi

BOOKGEN_JOB_COMMAND='python -m app.cli bookgen \
  --project-id "${PROJECT_ID}" \
  --run-date "${RUN_DATE}" \
  --bookspec-key "${BOOKSPEC_KEY}"'
if [[ "${WITH_ANALYTICS_REPORT}" == "true" ]]; then
  BOOKGEN_JOB_COMMAND="${BOOKGEN_JOB_COMMAND}
python -m app.cli bookgen-analytics-report --project-id \"\${PROJECT_ID}\""
fi
BOOKGEN_JOB_COMMAND_BLOCK="$(printf '%s\n' "${BOOKGEN_JOB_COMMAND}" | sed 's/^/              /')"
LAKEFS_ENV_BLOCK=""
if [[ -n "${LAKEFS_ENABLED_OVERRIDE}" ]]; then
  LAKEFS_ENV_BLOCK="$(cat <<EOF
            - name: LAKEFS_ENABLED
              value: "${LAKEFS_ENABLED_OVERRIDE}"
EOF
)"
fi

TMP_MANIFEST="$(mktemp)"
trap 'rm -f "${TMP_MANIFEST}"' EXIT

cat >"${TMP_MANIFEST}" <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: slw-bookgen
    app.kubernetes.io/part-of: slw-dailycast
    app.kubernetes.io/component: bookgen
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  activeDeadlineSeconds: 43200
  template:
    metadata:
      labels:
        app.kubernetes.io/name: slw-bookgen
        app.kubernetes.io/part-of: slw-dailycast
        app.kubernetes.io/component: bookgen
    spec:
      serviceAccountName: ${SERVICE_ACCOUNT}
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/arch: amd64
        slw/workload: ops
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: node-role.kubernetes.io/control-plane
                    operator: DoesNotExist
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: bookgen
          image: ${IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["sh", "-lc"]
          args:
            - |
${BOOKGEN_JOB_COMMAND_BLOCK}
          envFrom:
            - secretRef:
                name: ${SECRET_NAME}
            - configMapRef:
                name: ${CONFIGMAP_NAME}
          env:
            - name: PROJECT_ID
              value: "${PROJECT_ID}"
            - name: RUN_DATE
              value: "${RUN_DATE}"
            - name: BOOKSPEC_KEY
              value: "${BOOKSPEC_KEY}"
            - name: BOOKGEN_USE_LLM
              value: "${BOOKGEN_USE_LLM}"
            - name: BOOKGEN_LLM_CHAPTER_LIMIT
              value: "${BOOKGEN_LLM_CHAPTER_LIMIT}"
            - name: BOOKGEN_EVAL_USE_LLM
              value: "${BOOKGEN_EVAL_USE_LLM}"
            - name: BOOKGEN_REWRITE_USE_LLM
              value: "${BOOKGEN_REWRITE_USE_LLM}"
            - name: BOOKGEN_ALLOW_LOCK_OVERRIDE
              value: "${BOOKGEN_ALLOW_LOCK_OVERRIDE}"
            - name: BOOKGEN_FORCE_REDRAFT
              value: "${BOOKGEN_FORCE_REDRAFT}"
            - name: BOOKGEN_EVAL_LLM_CHAPTER_LIMIT
              value: "0"
            - name: BOOKGEN_REWRITE_LLM_CHAPTER_LIMIT
              value: "0"
            - name: LLM_TIMEOUT_SECONDS
              value: "${LLM_TIMEOUT_SECONDS}"
            - name: LLM_MAX_RETRIES
              value: "${LLM_MAX_RETRIES}"
${LAKEFS_ENV_BLOCK}
          resources:
            requests:
              cpu: ${CPU_REQUEST}
              memory: ${MEM_REQUEST}
            limits:
              cpu: "${CPU_LIMIT}"
              memory: ${MEM_LIMIT}
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            - name: var-tmp
              mountPath: /var/tmp
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
      volumes:
        - name: tmp
          emptyDir: {}
        - name: var-tmp
          emptyDir: {}
EOF

kubectl apply -f "${TMP_MANIFEST}"
echo "Created job ${JOB_NAME} in namespace ${NAMESPACE}"
echo "Watch: kubectl -n ${NAMESPACE} get job ${JOB_NAME} -w"
echo "Logs : kubectl -n ${NAMESPACE} logs -f job/${JOB_NAME}"

if [[ "${WAIT_MODE}" == "true" ]]; then
  started_at="$(date +%s)"
  while true; do
    succeeded="$(kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)"
    failed="$(kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
    active="$(kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o jsonpath='{.status.active}' 2>/dev/null || echo 0)"

    succeeded="${succeeded:-0}"
    failed="${failed:-0}"
    active="${active:-0}"

    if [[ "${succeeded}" =~ ^[0-9]+$ ]] && [[ "${succeeded}" -gt 0 ]]; then
      echo "Job ${JOB_NAME} completed successfully."
      break
    fi
    if [[ "${failed}" =~ ^[0-9]+$ ]] && [[ "${failed}" -gt 0 ]]; then
      echo "Job ${JOB_NAME} failed."
      kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o wide || true
      kubectl -n "${NAMESPACE}" logs "job/${JOB_NAME}" --tail=200 || true
      exit 1
    fi

    now="$(date +%s)"
    elapsed="$(( now - started_at ))"
    if [[ "${elapsed}" -ge "${WAIT_TIMEOUT_SECONDS}" ]]; then
      echo "Timed out waiting for job ${JOB_NAME} after ${WAIT_TIMEOUT_SECONDS}s."
      kubectl -n "${NAMESPACE}" get job "${JOB_NAME}" -o wide || true
      exit 1
    fi

    if [[ "${active}" =~ ^[0-9]+$ ]] && [[ "${active}" -gt 0 ]]; then
      sleep 5
    else
      sleep 3
    fi
  done
fi
