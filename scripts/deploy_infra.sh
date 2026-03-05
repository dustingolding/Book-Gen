#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${1:-sideline-wire-dailycast}"

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.template.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/networkpolicy.yaml

kubectl -n "${NAMESPACE}" rollout status statefulset/postgres --timeout=240s
kubectl -n "${NAMESPACE}" rollout status statefulset/minio --timeout=240s

echo
kubectl -n "${NAMESPACE}" get pods,svc,pvc,statefulset
