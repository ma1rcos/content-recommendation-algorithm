# Algoritmo de recomendação

Implementação: `services/recommendation-engine-python/app/recommender/` (pipeline
puro, sem I/O, testado com dados em memória) e `app/feed_builder.py` (carrega
os dados do Redis/PostgreSQL e grava o resultado).

O algoritmo é um **ranking híbrido linear** sobre sinais normalizados — a mesma
família de abordagem usada como baseline em sistemas reais, sem modelos
treinados. Ele não reproduz o ranking proprietário de nenhuma plataforma.

```
candidatos (6 fontes, com limites) → filtros obrigatórios → 8 features em [0,1]
  → score linear com pesos → top-K → reordenação por diversidade → feed (300 itens)
```

## 1. Perfil do usuário

O perfil tem três mapas de pesos — categoria, tag e autor — mais a lista de
interações recentes, o conjunto "já visto" e a exposição (impressões).

Cada interação contribui com um peso por tipo, com **decaimento exponencial no tempo**:

```
peso(T) = Σ w_tipo · 0.5^((T − t_i) / meia_vida)        meia_vida = 14 dias
```

| Tipo | Peso | Observação |
|---|---|---|
| `content_viewed` | 1 | visualização real (não é impressão) |
| `content_liked` | 3 | |
| `content_commented` | 4 | |
| `content_saved` | 5 | |
| `content_shared` | 6 | sinal explícito mais forte |
| `feed_impression` | — | **não altera o perfil**; só alimenta a penalidade de exposição |

Tags recebem metade do peso da interação; o autor recebe o peso inteiro.
Interesses declarados no cadastro entram com peso 1 por categoria (base do cold start).

**Atualização incremental** (script Lua `apply_interaction.lua`, atômico): em vez
de recalcular a soma, o perfil armazenado é decaído até "agora"
(`ZUNIONSTORE key 1 key WEIGHTS fator`, O(N) com N ≤ 200) e a nova contribuição é
somada. Matematicamente equivale à soma completa — `tests/test_integration.py::
test_incremental_profile_matches_full_rebuild` compara as duas formas.

Os mapas têm tamanho máximo (50 categorias, 100 tags, 200 autores): os menores
pesos são descartados.

## 2. Geração de candidatos

| Fonte | Como | Limite padrão |
|---|---|---|
| `author` | 10 publicações mais recentes dos 15 autores com maior afinidade | 150 |
| `category` | 60 mais recentes das 4 categorias de maior peso | 240 |
| `similar` | 20 mais recentes de cada uma das 6 tags dos itens consumidos por último | 120 |
| `trending` | top 150 do trending (24 h, meia-vida 6 h) | 150 |
| `explore` | conteúdos recentes de autores que o usuário ainda **não** conhece | 80 |

As listas são intercaladas (round-robin) e deduplicadas guardando **todas** as
fontes de cada item; o teto global é 800 candidatos por usuário. O intercalamento
evita que o teto descarte uma fonte inteira só por ela vir por último.

## 3. Filtros obrigatórios (independentes do score)

Removidos antes de qualquer cálculo — nada aqui é "penalizado":

- conteúdo removido/inexistente (`status != active`);
- autor bloqueado pelo usuário;
- já visto (visualizado ou com interação) nos últimos 30 dias (até 500 itens).

A API Rust repete o filtro **na entrega**, com o conjunto "já visto" atual: um item
visto depois que o feed foi gerado não volta a aparecer, sem regenerar o feed.

## 4. Features (todas em [0, 1])

| Feature | Definição |
|---|---|
| `interest` | `0.7 · peso_categoria/max + 0.3 · média(peso_tag/max)` |
| `author` | `peso_autor / max_peso_autor` |
| `engagement` | taxa por visualização com **suavização bayesiana**: `s = (eng + m·p)/(views + m)`, feature = `s/(s+p)`; `p` = taxa global, `m = 20`. Vale 0,5 na média global e não explode com 1 visualização sortuda |
| `popularity` | `log(1+trending) / log(1+max_trending_dos_candidatos)` — escala logarítmica: 10× mais eventos não dá 10× mais score |
| `recency` | `0.5^(idade_h / 48)` |
| `similarity` | maior Jaccard de tags com os itens consumidos recentemente (se a categoria for a mesma: `0,8·Jaccard + 0,2`) |
| `exploration` | se autor desconhecido **e** (conteúdo novo ou com < 30 views, ou veio da fonte `explore`): `0.5 + 0.5·u`, com `u` pseudoaleatório **determinístico** por (usuário, conteúdo, dia) |
| `exposure` | `1 − e^(−impressões/3)`: penaliza o que já foi exibido várias vezes sem visualização |

Normalizar tudo para [0, 1] é o que torna os pesos comparáveis: sem isso, uma
contagem bruta de likes dominaria a soma.

## 5. Score e ranking

```
score = 0.30·interest + 0.20·author + 0.15·engagement + 0.10·popularity
      + 0.15·recency + 0.10·similarity + 0.06·exploration − 0.25·exposure
```

- **Cold start** (usuário sem interações): pesos alternativos
  `interest 0.30, engagement 0.25, popularity 0.25, recency 0.20, exploration 0.10`
  (afinidade de autor e similaridade valem zero sem histórico).
- **Top-K**: `heapq.nsmallest` seleciona os 900 melhores (3× o feed) em O(n log k),
  sem ordenar a lista inteira.
- **Desempate explícito**: score ↓, data de publicação ↓, `content_id` ↑.
- **Determinismo**: mesmo perfil, candidatos, parâmetros e instante → mesmo feed
  (`test_ranking_is_deterministic`, `test_recovery_after_redis_loss_reproduces_feed`).

O ranking não depende só de popularidade: `test_global_popularity_does_not_override_interests`
verifica que conteúdo viral fora dos interesses não toma o feed de quem tem
interesse claro.

## 6. Reordenação por diversidade

Passo guloso sobre a lista ranqueada: em cada posição, escolhe o primeiro item
(em ordem de score, olhando até 60 à frente) que respeita todas as regras:

| Regra | Padrão |
|---|---|
| mesmo autor não reaparece em menos de N posições | 4 |
| no máximo M itens da mesma categoria em qualquer janela de W | 3 em 6 |
| fatia máxima do feed por autor | 8% (mín. 2 itens) |
| item muito parecido com o anterior (Jaccard de tags) | > 0,6 evitado |
| slot de descoberta | 1 a cada 8 posições, preenchido com o melhor item de exploração |

Se nenhum item atender, as regras são relaxadas em ordem (similaridade →
categoria → intervalo de autor → cota de autor), para o feed nunca encolher.

**Score individual × ordem final:** a diversidade não altera o score de nenhum
item (`test_individual_scores_are_not_changed`); ela só muda posições. Por isso o
`score` devolvido pela API não é monotônico ao longo do feed: um item com 0,62
pode aparecer antes de outro com 0,66 porque o segundo seria o terceiro item
seguido da mesma categoria.

## 7. Cold start

- **Usuário novo com interesses declarados**: categorias declaradas viram o perfil
  inicial (peso 1); fontes `category`, `trending` e `explore`; pesos de cold start.
- **Usuário novo sem nada**: `trending` + `explore` + recência.
- **Conteúdo novo**: entra nos pools no momento da publicação (evento
  `content_published`); o prior bayesiano dá engajamento 0,5 (neutro) em vez de 0;
  a feature de exploração favorece autores desconhecidos com pouca exposição; e o
  slot de descoberta garante espaço no feed.

Assim que o usuário interage, `count > 0` e os pesos personalizados passam a valer.

## 8. Atualização após interações

1. Evento entra em `rec:events` (resposta 202 imediata).
2. Worker grava no PostgreSQL (idempotente) e aplica o Lua: estatísticas, trending,
   "já visto", perfil com decaimento, **marca o feed como stale** e enfileira **um**
   job de regeneração por usuário (deduplicado).
3. Worker de regeneração libera a deduplicação e gera o feed novo; interações que
   chegarem durante a geração enfileiram outro job (coalescido).
4. Enquanto isso a API continua servindo o feed anterior (`source: cache_stale`).

Nenhuma interação recalcula feeds de outros usuários.

## 9. Como alterar os pesos e verificar o efeito

Pesos ficam em `app/config.py` (`RankingWeights`) e podem ser sobrescritos sem
mudar código:

```bash
# .env (parcial: chaves omitidas mantêm o padrão; valores entre 0 e 10)
RANKING_WEIGHTS={"recency":0.4,"interest":0.2}
# ou um arquivo JSON montado no container
RANKING_WEIGHTS_FILE=/config/weights.json
```

**Antes de aplicar**, compare sem gravar nada:

```bash
docker compose exec engine python -m app.explain --user 2 \
  --weights '{"recency":1.5,"interest":0.05,"author":0.05,"similarity":0}'
```

Saída real (seed padrão, usuária `bruno_chef`):

```
sobreposição do top 20: 3/20 (Jaccard 0.08)
[atual] categorias: {'culinaria': 11, 'viagem': 9}
[alternativo] categorias: {'pets': 4, 'games': 4, 'culinaria': 3, 'arte': 3, 'musica': 2, ...}
```

Para **aplicar**: altere o `.env`, `docker compose up -d engine worker-rebuild` e
force a regeneração (`POST /v1/recommendations/rebuild/{user_id}`), ou espere os
feeds expirarem (TTL de 1 h; soft TTL de 10 min dispara regeneração em background).
Compare `GET /v1/feed/{id}` antes/depois — o campo `feed_version` muda e o campo
`reason` de cada item mostra qual sinal dominou.

Testes que garantem que pesos mudam o resultado: `test_changing_weights_changes_results`.

## 10. Limitações do algoritmo

- Sem aprendizado: pesos fixos, ajustados à mão. O próximo passo natural é
  aprender os pesos (regressão logística / learning-to-rank) a partir de impressões
  e interações, que já são registradas.
- Similaridade por tags (Jaccard), não por embeddings.
- Interesses recentes reagem rápido: poucas interações fortes (ex.: 6 itens com
  like+save+share) mudam bastante o topo do feed, porque afinidade de autor e
  similaridade com os itens recentes somam ao interesse por categoria. É um
  trade-off ajustável pelos pesos `author` e `similarity`.
- O prior do engajamento usa visualizações como denominador (impressões não são
  persistidas, então não sobrevivem a uma perda do Redis).
