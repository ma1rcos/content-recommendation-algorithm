// Cenários de carga (k6). Um cenário por execução, escolhido por SCENARIO:
//
//   k6 run -e SCENARIO=warm tests/load/scenarios.js
//
// Variáveis: API (padrão http://localhost:8080), AUTH_SECRET (mesmo da API),
// RATE (req/s alvo), DURATION, USERS (faixa de usuários do seed).
//
// Cenários:
//   warm       feed com cache aquecido, usuários aleatórios entre 1..WARM_USERS
//   cold       primeira requisição de usuários nunca servidos (cache vazio → motor)
//   hot_user   pico de requisições para o MESMO usuário (single-flight / cache)
//   many_users feed para usuários aleatórios em toda a base (mistura hit/miss)
//   events     envio de eventos em lote (20 eventos por requisição)
//   ramp       aumento gradual de concorrência até RATE_MAX
// Para "indisponibilidade de dependência", rode `cold` com o motor parado
// (ver README): as respostas devem vir de fallback, rápidas e sem erro.

import http from 'k6/http';
import crypto from 'k6/crypto';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const API = __ENV.API || 'http://localhost:8080';
const SECRET = __ENV.AUTH_SECRET || 'local-demo-secret-local-demo-secret-0123';
const SCENARIO = __ENV.SCENARIO || 'warm';
const RATE = parseInt(__ENV.RATE || '500');
const DURATION = __ENV.DURATION || '60s';
const USERS = parseInt(__ENV.USERS || '2000');
const WARM_USERS = parseInt(__ENV.WARM_USERS || '1000');
const COLD_START = parseInt(__ENV.COLD_START || '1001');

const sources = new Counter('feed_source');

function token(uid) {
  const payload = `u.${uid}.${Math.floor(Date.now() / 1000) + 3600}`;
  return `${payload}.${crypto.hmac('sha256', SECRET, payload, 'hex')}`;
}
const tokens = {};
function auth(uid, name = 'feed') {
  if (!tokens[uid]) tokens[uid] = token(uid);
  return { headers: { Authorization: `Bearer ${tokens[uid]}`, 'Content-Type': 'application/json' }, tags: { name } };
}

function getFeed(uid) {
  const res = http.get(`${API}/v1/feed/${uid}?limit=20`, auth(uid));
  const ok = check(res, { 'status 200': (r) => r.status === 200 });
  // origem lida do header: o corpo é descartado para o gerador de carga gastar menos CPU
  if (ok) sources.add(1, { source: res.headers['X-Feed-Source'] || 'unknown' });
  return res;
}

const arrival = (rate, duration) => ({
  executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration,
  preAllocatedVUs: Math.max(20, Math.ceil(rate / 10)), maxVUs: Math.max(100, rate),
});

const SCENARIOS = {
  warm: { s: arrival(RATE, DURATION), t: { 'http_req_duration{name:feed}': ['p(95)<25', 'p(99)<50'] } },
  many_users: { s: arrival(RATE, DURATION), t: { 'http_req_duration{name:feed}': ['p(95)<100'] } },
  hot_user: { s: arrival(RATE, DURATION), t: { 'http_req_duration{name:feed}': ['p(95)<25'] } },
  cold: {
    s: { executor: 'shared-iterations', vus: parseInt(__ENV.VUS || '20'), iterations: parseInt(__ENV.ITERATIONS || '500'), maxDuration: '5m' },
    t: { 'http_req_duration{name:feed}': ['p(95)<1000'] },
  },
  events: { s: arrival(RATE, DURATION), t: { 'http_req_duration{name:events}': ['p(95)<25'] } },
  ramp: {
    s: {
      executor: 'ramping-arrival-rate', startRate: 50, timeUnit: '1s',
      preAllocatedVUs: 50, maxVUs: 1000,
      stages: [
        { target: Math.floor(parseInt(__ENV.RATE_MAX || '3000') / 3), duration: '30s' },
        { target: Math.floor((parseInt(__ENV.RATE_MAX || '3000') * 2) / 3), duration: '30s' },
        { target: parseInt(__ENV.RATE_MAX || '3000'), duration: '30s' },
        { target: parseInt(__ENV.RATE_MAX || '3000'), duration: '30s' },
      ],
    },
    t: {},
  },
};

const chosen = SCENARIOS[SCENARIO];
if (!chosen) throw new Error(`SCENARIO desconhecido: ${SCENARIO}`);

export const options = {
  scenarios: { [SCENARIO]: Object.assign({ exec: 'run' }, chosen.s) },
  thresholds: Object.assign({ http_req_failed: ['rate<0.01'] }, chosen.t),
  setupTimeout: '10m',
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
};

export function setup() {
  if (SCENARIO === 'warm' || SCENARIO === 'hot_user' || SCENARIO === 'ramp') {
    // Aquece os feeds antes de medir. Em lotes pequenos (≈ ENGINE_MAX_CONCURRENCY):
    // misses além disso recebem fallback e a geração vai para o worker, então
    // repetimos até todos os usuários terem feed próprio em cache.
    const n = SCENARIO === 'hot_user' ? 1 : WARM_USERS;
    const size = parseInt(__ENV.WARMUP_BATCH || '4');
    let pending = Array.from({ length: n }, (_, i) => i + 1);
    for (let pass = 0; pass < 20 && pending.length > 0; pass++) {
      const still = [];
      for (let i = 0; i < pending.length; i += size) {
        const ids = pending.slice(i, i + size);
        const res = http.batch(ids.map((uid) => ['GET', `${API}/v1/feed/${uid}?limit=20`, null, auth(uid, 'warmup')]));
        res.forEach((r, j) => {
          const src = r.headers['X-Feed-Source'];
          if (src !== 'cache' && src !== 'built' && src !== 'cache_stale') still.push(ids[j]);
        });
      }
      pending = still;
      if (pending.length > 0) sleep(2);
    }
    console.log(`aquecimento concluído; sem feed próprio: ${pending.length}`);
  }
}

const rand = (n) => 1 + Math.floor(Math.random() * n);

export function run() {
  switch (SCENARIO) {
    case 'warm': case 'ramp': getFeed(rand(WARM_USERS)); break;
    case 'many_users': getFeed(rand(USERS)); break;
    case 'hot_user': getFeed(1); break;
    case 'cold': getFeed(COLD_START + exec.scenario.iterationInTest); break;
    case 'events': {
      const uid = rand(USERS);
      const events = [];
      for (let i = 0; i < 20; i++) {
        events.push({ type: i % 5 === 0 ? 'content_liked' : 'content_viewed', user_id: uid, content_id: rand(20000) });
      }
      const res = http.post(`${API}/v1/events`, JSON.stringify({ events }), auth(uid, 'events'));
      check(res, { 'status 202': (r) => r.status === 202 });
      break;
    }
  }
}
