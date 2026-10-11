-- Aplica uma interação (view/like/comment/save/share) de forma atômica e idempotente.
--
-- KEYS[1]  rec:evt:{event_id}          chave de idempotência
-- KEYS[2]  rec:stats:{cid}             contadores do conteúdo
-- KEYS[3]  rec:trend:{hora}            bucket horário de trending
-- KEYS[4]  rec:stats:global            prior global de engajamento
-- KEYS[5]  rec:profile:meta:{uid}
-- KEYS[6]  rec:profile:cat:{uid}
-- KEYS[7]  rec:profile:tag:{uid}
-- KEYS[8]  rec:profile:author:{uid}
-- KEYS[9]  rec:seen:{uid}
-- KEYS[10] rec:recent:{uid}
-- KEYS[11] rec:feed:meta:{uid}
-- KEYS[12] rec:job:pending:{uid}
-- KEYS[13] rec:jobs:rebuild            stream de jobs
--
-- ARGV: 1 content_id, 2 user_id, 3 event_ts (s), 4 now (s), 5 ingested_at_ms,
--       6 peso, 7 stat_field, 8 explícito (1/0), 9 categoria, 10 autor,
--       11 tags (csv), 12 meia-vida (s), 13 idempotency_ttl, 14 trend_ttl,
--       15 seen_cap, 16 seen_ttl, 17 recent_cap, 18 profile_ttl,
--       19 cap_cat, 20 cap_tag, 21 cap_author, 22 jobs_maxlen, 23 janela de dedup (s)
--
-- Retorno: 0 = duplicado (no-op), 1 = aplicado, 2 = aplicado sem perfil
-- (perfil ausente ou reconstruído depois deste evento — já o contém).

if redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[13])) == false then
  return 0
end

local cid = ARGV[1]
local uid = ARGV[2]
local event_ts = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local weight = tonumber(ARGV[6])
local explicit = ARGV[8] == '1'

-- estatísticas do conteúdo e prior global
redis.call('HINCRBY', KEYS[2], ARGV[7], 1)
if explicit then
  redis.call('HINCRBYFLOAT', KEYS[2], 'eng', weight)
  redis.call('HINCRBYFLOAT', KEYS[4], 'eng', weight)
else
  redis.call('HINCRBY', KEYS[4], 'views', 1)
end
redis.call('ZINCRBY', KEYS[3], weight, cid)
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[14]))

-- histórico recente (usado como filtro "já visto")
redis.call('ZADD', KEYS[9], event_ts, cid)
redis.call('ZREMRANGEBYRANK', KEYS[9], 0, -(tonumber(ARGV[15]) + 1))
redis.call('EXPIRE', KEYS[9], tonumber(ARGV[16]))

local result = 2
local meta = redis.call('HMGET', KEYS[5], 'ts', 'rebuilt_at_ms')
-- Só atualiza o perfil se ele existe e se este evento ainda não estava no
-- snapshot usado para reconstruí-lo (margem de 2 s, ver ARCHITECTURE.md).
if meta[1] and tonumber(ARGV[5]) >= (tonumber(meta[2] or '0') - 2000) then
  local half = tonumber(ARGV[12])
  local last = tonumber(meta[1])
  if now > last then
    local factor = math.pow(0.5, (now - last) / half)
    if factor < 0.9999 then
      for i = 6, 8 do
        if redis.call('EXISTS', KEYS[i]) == 1 then
          -- ZUNIONSTORE com WEIGHTS escala o ZSET inteiro in-place em O(N).
          redis.call('ZUNIONSTORE', KEYS[i], 1, KEYS[i], 'WEIGHTS', factor)
        end
      end
    end
  end
  local contrib = weight * math.pow(0.5, math.max(0, now - event_ts) / half)
  redis.call('ZINCRBY', KEYS[6], contrib, ARGV[9])
  redis.call('ZINCRBY', KEYS[8], contrib, ARGV[10])
  for tag in string.gmatch(ARGV[11], '[^,]+') do
    redis.call('ZINCRBY', KEYS[7], contrib * 0.5, tag)
  end
  redis.call('ZREMRANGEBYRANK', KEYS[6], 0, -(tonumber(ARGV[19]) + 1))
  redis.call('ZREMRANGEBYRANK', KEYS[7], 0, -(tonumber(ARGV[20]) + 1))
  redis.call('ZREMRANGEBYRANK', KEYS[8], 0, -(tonumber(ARGV[21]) + 1))
  redis.call('ZADD', KEYS[10], event_ts, cid)
  redis.call('ZREMRANGEBYRANK', KEYS[10], 0, -(tonumber(ARGV[17]) + 1))
  if now > last then
    redis.call('HSET', KEYS[5], 'ts', now)
  end
  redis.call('HINCRBY', KEYS[5], 'count', 1)
  local ttl = tonumber(ARGV[18])
  for i = 5, 8 do redis.call('EXPIRE', KEYS[i], ttl) end
  redis.call('EXPIRE', KEYS[10], ttl)
  result = 1
end

-- Invalidação seletiva: marca o feed como desatualizado (stale-while-revalidate)
-- e enfileira no máximo UMA regeneração por usuário por janela de dedup.
if explicit and redis.call('EXISTS', KEYS[11]) == 1 then
  redis.call('HSET', KEYS[11], 'stale', 1, 'stale_at_ms', tonumber(ARGV[5]))
  if redis.call('SET', KEYS[12], now, 'NX', 'EX', tonumber(ARGV[23])) then
    redis.call('XADD', KEYS[13], 'MAXLEN', '~', tonumber(ARGV[22]), '*',
               'user_id', uid, 'reason', 'interaction', 'ts', now)
  end
end
return result
