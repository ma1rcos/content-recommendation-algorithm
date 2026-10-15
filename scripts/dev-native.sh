#!/usr/bin/env bash
# Sobe motor, worker e API SEM Docker (Redis e PostgreSQL locais já no ar).
# Usado para desenvolvimento e para os testes de carga documentados no README.
#   ./scripts/dev-native.sh start|stop
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="${LOG_DIR:-$ROOT/.native-logs}"; mkdir -p "$LOG"
: "${APP_DATABASE_URL:=postgresql://reco_app:app_dev_pw@127.0.0.1:5432/reco}"
: "${REDIS_URL:=redis://127.0.0.1:6379/0}"
: "${INTERNAL_TOKEN:=local-internal-token}"
: "${AUTH_SECRET:=local-demo-secret-local-demo-secret-0123}"
export REDIS_URL INTERNAL_TOKEN AUTH_SECRET
case "${1:-start}" in
  start)
    cd "$ROOT/services/recommendation-engine-python"
    DATABASE_URL="$APP_DATABASE_URL" ENGINE_HOST=127.0.0.1 PATH="$PWD/.venv/bin:$PATH" \
      nohup ./run-engine.sh >"$LOG/engine.log" 2>&1 & echo $! >"$LOG/engine.pid"
    DATABASE_URL="$APP_DATABASE_URL" nohup .venv/bin/python -m workers.main --role events --metrics-port 9100 \
      >"$LOG/worker.log" 2>&1 & echo $! >"$LOG/worker.pid"
    DATABASE_URL="$APP_DATABASE_URL" nohup .venv/bin/python -m workers.main --role rebuild --metrics-port 9101 \
      >"$LOG/worker-rebuild.log" 2>&1 & echo $! >"$LOG/worker-rebuild.pid"
    cd "$ROOT/services/recommendation-api-rust"
    DATABASE_URL="$APP_DATABASE_URL" ENGINE_URL=http://127.0.0.1:8000 BIND_ADDR=127.0.0.1:8080 \
      nohup ./target/release/recommendation-api >"$LOG/api.log" 2>&1 & echo $! >"$LOG/api.pid"
    sleep 2; curl -fsS http://127.0.0.1:8080/ready && echo ;;
  stop)
    for s in api worker worker-rebuild engine; do [ -f "$LOG/$s.pid" ] && kill "$(cat "$LOG/$s.pid")" 2>/dev/null || true; rm -f "$LOG/$s.pid"; done ;;
esac
