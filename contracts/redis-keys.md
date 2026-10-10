# Contrato de chaves Redis

Todas as chaves usam o prefixo `rec:`. A definição em código está em
`services/recommendation-engine-python/app/keys.py` (escritor principal) e
`services/recommendation-api-rust/src/keys.rs` (leitor). Os testes de integração
dos dois lados gravam/leem estes formatos.

Política de memória do Redis: `volatile-lru`. Só chaves **com TTL** podem ser
despejadas sob pressão de memória; catálogo, pools e streams não têm TTL e são
limitados por tamanho (`cap`). Tudo aqui é **derivado**: a fonte de verdade é o
PostgreSQL (exceto impressões/exposição, que existem só no Redis).

| Chave | Tipo | Conteúdo | Limite / TTL | Escrita por | Lida por |
|---|---|---|---|---|---|
| `rec:catalog:{cid}` | Hash | `author_id`, `category`, `tags` (csv), `title`, `published_at` (epoch s), `status` | 1 por conteúdo, sem TTL | worker, seed/rebuild | motor, API |
| `rec:catalog:ready` | String | marcador de catálogo carregado (epoch s) | sem TTL | rebuild | motor, worker |
| `rec:stats:{cid}` | Hash | `views`, `likes`, `comments`, `saves`, `shares`, `impressions`, `eng` (engajamento ponderado) | sem TTL | Lua (worker) | motor, API |
| `rec:stats:global` | Hash | `views`, `eng`, `impressions` — prior global da taxa de engajamento | — | Lua | motor |
| `rec:pool:recent` | ZSET | cid → published_at | 5000 mais recentes | worker | motor (exploração) |
| `rec:pool:cat:{slug}` | ZSET | cid → published_at | 1000 por categoria | worker | motor |
| `rec:pool:author:{aid}` | ZSET | cid → published_at | 100 por autor | worker | motor |
| `rec:pool:tag:{slug}` | ZSET | cid → published_at | 500 por tag | worker | motor |
| `rec:trend:{hora}` | ZSET | cid → soma ponderada de interações na hora (`hora = epoch // 3600`) | TTL 26 h | Lua | manutenção |
| `rec:trending` | ZSET | cid → score com decaimento (meia-vida 6 h, janela 24 h) | top 1000, refeito a cada 30 s | manutenção | motor, API (fallback) |
| `rec:profile:cat:{uid}` | ZSET | categoria → peso com decaimento | 50, TTL 90 d | Lua, rebuild | motor, API |
| `rec:profile:tag:{uid}` | ZSET | tag → peso | 100, TTL 90 d | Lua, rebuild | motor, API |
| `rec:profile:author:{uid}` | ZSET | autor → peso | 200, TTL 90 d | Lua, rebuild | motor, API |
| `rec:profile:meta:{uid}` | Hash | `ts` (referência do decaimento), `count`, `blocked` (csv), `rebuilt_at_ms` | TTL 90 d | Lua, rebuild | motor, API |
| `rec:recent:{uid}` | ZSET | cid → ts das interações recentes | 20, TTL 90 d | Lua, rebuild | motor (similaridade) |
| `rec:seen:{uid}` | ZSET | cid → ts (visualizado/interagido) | 500, TTL 30 d | Lua, rebuild | motor, API (filtro) |
| `rec:exposure:{uid}` | ZSET | cid → nº de impressões | 1000, TTL 7 d | Lua (impressão) | motor (penalidade) |
| `rec:feed:{uid}` | ZSET | cid → posição invertida (ZREVRANGE = ordem final) | `FEED_SIZE` (300), TTL 1 h ± 10% | motor | API |
| `rec:feed:info:{uid}` | Hash | cid → `"score|motivo"` | TTL do feed | motor | API |
| `rec:feed:meta:{uid}` | Hash | `version`, `generated_at_ms`, `stale`, `stale_at_ms`, `size`, `strategy` | TTL do feed | motor, Lua | API, worker |
| `rec:lock:build:{uid}` | String | token do dono (single-flight da geração) | TTL 15–30 s | API, worker | — |
| `rec:job:pending:{uid}` | String | deduplicação de jobs de regeneração | TTL `REBUILD_DEDUP_SECONDS`; apagada quando o job começa | API, Lua | worker |
| `rec:evt:{event_id}` | String | evento já aplicado (idempotência) | TTL 7 d | Lua | Lua |
| `rec:rl:{escopo}:{quem}:{janela}` | String | contador de rate limit (janela fixa) | TTL janela + 1 s | API | API |
| `rec:lock:catalog`, `rec:lock:maintenance` | String | locks de jobs periódicos | TTL | worker | worker |

## Streams

| Stream | Grupo | Campos | Produtor | Consumidor |
|---|---|---|---|---|
| `rec:events` | `rec-workers` | `payload` = JSON (`contracts/schemas/event-envelope.schema.json`) | API | worker `--role events` |
| `rec:jobs:rebuild` | `rec-builders` | `user_id`, `reason` (`interaction`, `stale`, `api`, `miss_*`), `ts` | API, Lua | worker `--role rebuild` |
| `rec:events:dlq` | — | `original_id`, `reason`, `payload` | worker | operação (inspeção manual) |

`MAXLEN ~` aproximado: `rec:events` 1.000.000, `rec:jobs:rebuild` 100.000,
`rec:events:dlq` 10.000. Se o worker ficar tão atrasado que o stream atinja o
limite, as mensagens mais antigas **não processadas** são descartadas — monitore
`reco_worker_stream_lag` (ver docs/ARCHITECTURE.md).

## Redis Cluster

O projeto assume uma instância única. O script `apply_impression.lua` deriva
chaves `rec:stats:{cid}` internamente, o que não é permitido em Redis Cluster;
para cluster seria preciso passar todas as chaves em `KEYS` e usar hash tags
(`rec:{uid}:...`) para manter as chaves de um usuário no mesmo slot.
