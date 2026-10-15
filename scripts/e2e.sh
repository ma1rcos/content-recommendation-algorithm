#!/usr/bin/env bash
# Fluxo ponta a ponta (critérios de aceitação). Requer: curl, jq, python3 e a
# stack no ar com o seed aplicado (make up && make seed).
#
#   ./scripts/e2e.sh                     # contra http://localhost:8080
#   API=http://host:8080 REDIS_CLI="redis-cli -p 6379" ./scripts/e2e.sh
#
set -euo pipefail

API="${API:-http://localhost:8080}"
# redis-cli: com Docker Compose usa o container (senha do próprio ambiente dele);
# fora do Docker, defina REDIS_CLI="redis-cli -p 6379".
rcli() {
  if [ -n "${REDIS_CLI:-}" ]; then $REDIS_CLI "$@"
  else docker compose exec -T redis sh -c 'redis-cli -a "$REDIS_PASSWORD" --no-auth-warning "$@"' redis-cli "$@"; fi
}
export -f rcli; export REDIS_CLI
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
tok() { python3 "$ROOT/scripts/make_token.py" "$@"; }
U1="Authorization: Bearer $(tok user 1)"
U2="Authorization: Bearer $(tok user 2)"
ADMIN="Authorization: Bearer $(tok admin)"
pass=0
ok()   { pass=$((pass + 1)); printf '  \033[32m✔\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✘ %s\033[0m\n' "$*"; exit 1; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
feed() { curl -fsS -H "$1" "$API/v1/feed/$2?limit=${3:-20}"; }
cats() { jq -r '[.items[].category] | group_by(.) | map("\(.[0])=\(length)") | join(" ")'; }
share() { jq --arg c "$1" '(.items | length) as $n | [.items[] | select(.category == $c)] | length / ([$n, 1] | max)'; }
wait_for() { # wait_for <segundos> <comando...>: repete até o comando ter sucesso
  local t=$1; shift
  for _ in $(seq 1 "$t"); do if "$@" >/dev/null 2>&1; then return 0; fi; sleep 1; done; return 1
}

step "1. Saúde e prontidão"
curl -fsS "$API/health" | jq -e '.status == "ok"' >/dev/null && ok "/health"
curl -fsS "$API/ready" | jq -c . && ok "/ready"

step "2. Feeds personalizados para usuários diferentes"
f1=$(feed "$U1" 1); f2=$(feed "$U2" 2)
echo "  ana_games  ($(jq -r .source <<<"$f1")): $(cats <<<"$f1")"
echo "  bruno_chef ($(jq -r .source <<<"$f2")): $(cats <<<"$f2")"
common=$(jq -n --argjson a "$f1" --argjson b "$f2" '[$a.items[].content_id] - ([$a.items[].content_id] - [$b.items[].content_id]) | length')
[ "$common" -le 2 ] && ok "feeds diferentes (itens em comum no top 20: $common)" || fail "feeds parecidos demais ($common em comum)"

step "3. Segunda leitura vem do cache (sem ranking completo)"
src=$(feed "$U1" 1 | jq -r .source)
[[ "$src" == cache* ]] && ok "source=$src" || fail "esperava cache, veio $src"
next=$(feed "$U1" 1 10 | jq -r .next_cursor)
p2=$(curl -fsS -H "$U1" "$API/v1/feed/1?limit=10&cursor=$next")
[ "$(jq '.items | length' <<<"$p2")" -eq 10 ] && ok "paginação por cursor ($next)"

step "4. Interações mudam o perfil e o feed (processamento assíncrono)"
before_share=$(feed "$U1" 1 | share culinaria)
before_ver=$(feed "$U1" 1 | jq .feed_version)
targets=$(jq -c '[.items[] | select(.category == "culinaria") | .content_id][:6]' <<<"$f2")
echo "  ana_games interage com culinária: $targets (fatia antes: $before_share)"
payload=$(jq -c '{events: [.[] as $c | ("content_liked","content_saved","content_shared") as $t
                 | {type: $t, user_id: 1, content_id: $c}]}' <<<"$targets")
curl -fsS -X POST -H "$U1" -H 'content-type: application/json' -d "$payload" "$API/v1/events" | jq -c .
wait_for 20 bash -c "curl -fsS -H '$U1' '$API/v1/users/1/interests' | jq -e '[.categories[] | select(.key == \"culinaria\" and .weight >= 0.5)] | length == 1'" \
  && ok "perfil atualizado: $(curl -fsS -H "$U1" "$API/v1/users/1/interests" | jq -c '[.categories[:4][] | "\(.key):\(.weight)"]')" \
  || fail "perfil não refletiu as interações"
wait_for 30 bash -c "curl -fsS -H '$U1' '$API/v1/feed/1' | jq -e '.stale == false and .feed_version != $before_ver'" \
  && ok "feed regenerado em background (versão $before_ver → $(feed "$U1" 1 | jq .feed_version))" || fail "feed não foi regenerado"
after=$(feed "$U1" 1)
after_share=$(share culinaria <<<"$after")
echo "  ana_games agora: $(cats <<<"$after")"
jq -n "$after_share > $before_share" -e >/dev/null && ok "fatia de culinária no top 20: $before_share → $after_share" \
  || fail "interações não mudaram o feed"
jq -e --argjson t "$targets" '(.items | length) as $n | ([.items[].content_id] - $t | length) == $n' <<<"$after" >/dev/null \
  && ok "itens já interagidos não reaparecem"

step "5. Idempotência: reenviar o mesmo event_id não conta duas vezes"
cid=$(jq '.items[0].content_id' <<<"$f2")
eid=$(python3 -c 'import uuid; print(uuid.uuid4())')
ev="{\"events\":[{\"event_id\":\"$eid\",\"type\":\"content_liked\",\"user_id\":2,\"content_id\":$cid}]}"
likes0=$(curl -fsS -H "$U2" "$API/v1/content/$cid" | jq '.stats.likes // 0')
for _ in 1 2 3; do curl -fsS -X POST -H "$U2" -H 'content-type: application/json' -d "$ev" "$API/v1/events" >/dev/null; done
wait_for 15 bash -c "[ \"\$(curl -fsS -H '$U2' '$API/v1/content/$cid' | jq '.stats.likes // 0')\" -gt $likes0 ]"
sleep 2
likes1=$(curl -fsS -H "$U2" "$API/v1/content/$cid" | jq '.stats.likes // 0')
[ "$((likes1 - likes0))" -eq 1 ] && ok "3 envios do mesmo evento → likes $likes0 → $likes1" || fail "likes $likes0 → $likes1"

step "6. Novo conteúdo é indexado pelo worker"
new=$(curl -fsS -X POST -H "$ADMIN" -H 'content-type: application/json' \
  -d '{"author_id": 1, "category": "games", "title": "Speedrun ao vivo", "tags": ["speedrun", "ao-vivo"]}' "$API/v1/content")
ncid=$(jq .content_id <<<"$new")
ok "criado content_id=$ncid"
wait_for 15 bash -c "rcli EXISTS rec:catalog:$ncid | grep -q 1" && ok "catálogo e pools atualizados no Redis"

step "7. Cold start"
for u in 3 4; do
  f=$(feed "Authorization: Bearer $(tok user $u)" $u)
  echo "  usuário $u ($(jq -r .source <<<"$f")): $(cats <<<"$f")"
done
ok "usuários sem histórico recebem feed (interesses declarados + populares + descoberta)"

step "8. Perda do Redis: degradação controlada e reconstrução"
rcli FLUSHALL >/dev/null && ok "FLUSHALL executado (equivale a perder todos os dados do Redis)"
src=$(feed "$U1" 1 | jq -r .source)
echo "  logo após a perda: source=$src"
wait_for 60 bash -c "curl -fsS -H '$U1' '$API/v1/feed/1' | jq -e '.source == \"cache\" or .source == \"built\"'" \
  && ok "feed personalizado reconstruído a partir do PostgreSQL" || fail "feed não foi reconstruído"
rebuilt=$(feed "$U1" 1)
echo "  depois: $(cats <<<"$rebuilt")"
curl -fsS -H "$U1" "$API/v1/users/1/interests" | jq -e '.profile_loaded and .interaction_count > 0' >/dev/null \
  && ok "perfil reconstruído das interações persistidas (inclui as do passo 4)"

step "9. Métricas"
curl -fsS "$API/metrics" | grep -E '^reco_feed_responses_total' | sed 's/^/  /'
ok "métricas expostas"

printf '\n\033[32m%d verificações OK\033[0m\n' "$pass"
