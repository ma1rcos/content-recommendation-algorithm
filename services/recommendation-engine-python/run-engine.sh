#!/bin/sh
# Inicia o motor com ENGINE_WORKERS processos uvicorn. Com mais de um processo,
# as métricas Prometheus usam o modo multiprocesso (diretório limpo a cada início).
set -eu
WORKERS="${ENGINE_WORKERS:-1}"
if [ "$WORKERS" -gt 1 ]; then
  export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom-engine}"
  rm -rf "$PROMETHEUS_MULTIPROC_DIR" && mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi
exec uvicorn app.api:app --host "${ENGINE_HOST:-0.0.0.0}" --port "${ENGINE_PORT:-8000}" \
  --workers "$WORKERS" --timeout-graceful-shutdown 15 --no-access-log --log-level warning
