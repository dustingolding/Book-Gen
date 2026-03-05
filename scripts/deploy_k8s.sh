#!/usr/bin/env bash
set -euo pipefail

echo "[WARN] Raw k8s/jobs deployment is intended for testing/bootstrap."
echo "[INFO] For production scheduling and resource controls, use Helm:"
echo "       ./scripts/helm_deploy.sh <release> <namespace> <image-repo> <image-tag>"

kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/serviceaccount.yaml
kubectl apply -f k8s/secrets.template.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/postgres.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/prefect.yaml
kubectl apply -f k8s/networkpolicy.yaml
kubectl apply -f k8s/networkpolicy-prefect-ingress.yaml
kubectl apply -f k8s/networkpolicy-minio-ingress.yaml
kubectl apply -f k8s/jobs/pvc.yaml
kubectl apply -f k8s/jobs/

kubectl -n sideline-wire-dailycast get statefulset,svc,pvc,cronjob,job
