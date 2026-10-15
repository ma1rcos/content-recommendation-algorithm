#!/usr/bin/env bash
# Executa a bateria de carga e grava os resumos em tests/load/results/.
#
#   ./scripts/load-test.sh            # usa k6 local (ou K6="docker compose run --rm k6")
#
# Pré-requisitos: stack no ar e seed recém-aplicado com 2000 usuários. Faixas:
# 1–1000 aquecidos (warm/hot/ramp), 1001–1400 frios (cold), 1–1800 aleatórios
# (many_users/events) e 1801–2000 intocados até o cenário com o motor parado.
# STOP_ENGINE/START_ENGINE: comandos para o cenário de indisponibilidade do
# motor (padrão: docker compose stop/start engine).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
K6="${K6:-k6}"
OUT="tests/load/results"; mkdir -p "$OUT"
STOP_ENGINE="${STOP_ENGINE:-docker compose stop engine}"
START_ENGINE="${START_ENGINE:-docker compose start engine}"
SCRIPT="${SCRIPT_PATH:-tests/load/scenarios.js}"
API="${API:-http://localhost:8080}"   # acessível a partir deste host (métricas)
K6_API="${K6_API:-$API}"              # acessível a partir do k6 (ex.: http://api:8080 no Docker)
sources() { curl -fsS "$API/metrics" | grep '^reco_feed_responses_total' || true; }

run() { # run <nome> <args k6...>
  local name=$1; shift
  echo "== $name"
  local before; before=$(sources)
  $K6 run -q -e API="$K6_API" -e AUTH_SECRET="${AUTH_SECRET:-$(sed -n 's/^AUTH_SECRET=//p' .env 2>/dev/null)}" --summary-export "$OUT/$name.json" "$@" "$SCRIPT" >"$OUT/$name.txt" 2>&1 \
    || echo "   (thresholds violados — ver $OUT/$name.txt)"
  # Origem das respostas de feed durante o cenário (delta do contador da API).
  python3 - "$before" "$(sources)" >"$OUT/$name.sources.json" <<'PY'
import json, re, sys
parse = lambda t: {m[1]: float(m[2]) for m in re.finditer(r'source="([^"]+)"\} (\S+)', t)}
b, a = parse(sys.argv[1]), parse(sys.argv[2])
print(json.dumps({k: int(a[k] - b.get(k, 0)) for k in a if a[k] - b.get(k, 0) > 0}))
PY
}

run warm        -e SCENARIO=warm       -e RATE="${WARM_RATE:-1000}" -e DURATION=30s
run hot_user    -e SCENARIO=hot_user   -e RATE="${WARM_RATE:-1000}" -e DURATION=20s
run cold        -e SCENARIO=cold       -e COLD_START=1001 -e ITERATIONS=400 -e VUS=20
run many_users  -e SCENARIO=many_users -e RATE=300 -e DURATION=30s -e USERS=1800
run events      -e SCENARIO=events     -e RATE=200 -e DURATION=30s -e USERS=1800
run ramp        -e SCENARIO=ramp       -e RATE_MAX="${RAMP_MAX:-2000}"
echo "== engine_down (motor parado durante cache vazio)"
$STOP_ENGINE >/dev/null
run engine_down -e SCENARIO=cold       -e COLD_START=1801 -e ITERATIONS=200 -e VUS=20
$START_ENGINE >/dev/null

python3 - "$OUT" <<'EOF' | tee "$OUT/summary.txt"
import json, sys, pathlib
out = pathlib.Path(sys.argv[1])
# duração da fase medida (s) dos cenários de taxa constante; None = iterações fixas
durations = {"warm": 30, "hot_user": 20, "many_users": 30, "events": 30, "ramp": 120, "cold": None, "engine_down": None}
print(f"{'cenário':<12} {'iterações':>9} {'it/s':>7} {'descart.':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'erros':>7}  origem do feed")
for name, secs in durations.items():
    f = out / f"{name}.json"
    if not f.exists():
        continue
    m = json.loads(f.read_text())["metrics"]
    key = next((k for k in m if k.startswith("http_req_duration{name:") and "warmup" not in k), "http_req_duration")
    d, it, fail = m[key], m["iterations"], m["http_req_failed"]
    rate = it["count"] / secs if secs else it["rate"]
    dropped = m.get("dropped_iterations", {}).get("count", 0)
    src = (out / f"{name}.sources.json").read_text().strip() if (out / f"{name}.sources.json").exists() else ""
    print(f"{name:<12} {it['count']:>9} {rate:>7.0f} {dropped:>8} {d['med']:>8.2f} {d['p(95)']:>8.2f} {d['p(99)']:>8.2f} "
          f"{fail.get('value', fail.get('rate', 0)) * 100:>6.2f}%  {src}")
EOF
