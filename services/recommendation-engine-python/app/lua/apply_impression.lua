-- Registra uma impressão em lote (itens exibidos num feed) de forma idempotente.
-- Impressão é EXPOSIÇÃO, não interesse: não altera o perfil, só alimenta a
-- penalidade de exposição repetida e o contador de impressões dos conteúdos.
--
-- KEYS[1] rec:evt:{event_id}, KEYS[2] rec:exposure:{uid}, KEYS[3] rec:stats:global
-- ARGV: 1 idempotency_ttl, 2 exposure_cap, 3 exposure_ttl, 4.. content_ids
-- As chaves rec:stats:{cid} são derivadas aqui (instância única; ver ARCHITECTURE.md
-- sobre Redis Cluster).

if redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1])) == false then
  return 0
end
local n = 0
for i = 4, #ARGV do
  redis.call('ZINCRBY', KEYS[2], 1, ARGV[i])
  redis.call('HINCRBY', 'rec:stats:' .. ARGV[i], 'impressions', 1)
  n = n + 1
end
redis.call('HINCRBY', KEYS[3], 'impressions', n)
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, -(tonumber(ARGV[2]) + 1))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]))
return 1
