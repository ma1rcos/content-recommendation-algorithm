# Atalhos. Requer Docker Compose v2; os alvos de teste local requerem uv e cargo.
COMPOSE ?= docker compose
PY_DIR  := services/recommendation-engine-python
RS_DIR  := services/recommendation-api-rust
TEST_DATABASE_URL ?= postgresql://postgres:test@127.0.0.1:55432/reco_test
TEST_REDIS_URL    ?= redis://127.0.0.1:56379/15
TEST_REDIS_URL_RS ?= redis://127.0.0.1:56379/14

.PHONY: help env up down restart ps logs migrate seed token e2e scale \
        test test-deps test-deps-down test-python test-rust lint load clean

help: ## Lista os alvos
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

env: ## Cria .env a partir do exemplo (não sobrescreve)
	@test -f .env || (cp .env.example .env && echo ".env criado — troque os segredos")

up: env ## Constrói e sobe toda a stack (migrations rodam automaticamente)
	$(COMPOSE) up -d --build
	@echo "aguardando a API ficar pronta..."; for i in $$(seq 1 90); do \
	  curl -fsS http://127.0.0.1:$${API_PORT:-8080}/ready >/dev/null 2>&1 && { curl -fsS http://127.0.0.1:$${API_PORT:-8080}/ready; echo; exit 0; }; sleep 2; done; \
	  echo "API não ficou pronta — veja: make logs"; exit 1

down: ## Para a stack (mantém volumes)
	$(COMPOSE) down

restart: ## Reinicia os serviços de aplicação
	$(COMPOSE) restart api engine worker worker-rebuild

ps: ## Estado e saúde dos containers
	$(COMPOSE) ps

logs: ## Logs da API, motor e workers
	$(COMPOSE) logs -f --tail=100 api engine worker worker-rebuild

migrate: ## Aplica migrations pendentes
	$(COMPOSE) run --rm migrate

seed: ## Gera dados sintéticos (apaga os existentes). Ex.: make seed ARGS="--users 500"
	$(COMPOSE) run --rm seed --reset $(ARGS)

token: ## Token de demonstração. Ex.: make -s token ID=1 | make -s token ADMIN=1
	@if [ -n "$(ADMIN)" ]; then python3 scripts/make_token.py admin; else python3 scripts/make_token.py user $(or $(ID),1); fi

e2e: ## Fluxo ponta a ponta (critérios de aceitação) contra a stack no ar
	./scripts/e2e.sh

scale: ## Escala serviços. Ex.: make scale API=3 WORKERS=2 REBUILDERS=2
	$(COMPOSE) up -d --no-recreate --scale api=$(or $(API),1) --scale worker=$(or $(WORKERS),1) \
	  --scale worker-rebuild=$(or $(REBUILDERS),1) --scale engine=$(or $(ENGINES),1)

test-deps: ## Sobe Redis/PostgreSQL descartáveis para testes
	$(COMPOSE) -f docker-compose.test.yml up -d --wait

test-deps-down:
	$(COMPOSE) -f docker-compose.test.yml down

test-python: ## Testes Python (unitários, contrato e integração)
	cd $(PY_DIR) && uv sync --frozen && TEST_DATABASE_URL=$(TEST_DATABASE_URL) TEST_REDIS_URL=$(TEST_REDIS_URL) uv run pytest -q

test-rust: ## Testes Rust (unitários, contrato e integração; usa o banco do test-python)
	cd $(RS_DIR) && TEST_DATABASE_URL=$(TEST_DATABASE_URL) TEST_REDIS_URL=$(TEST_REDIS_URL_RS) cargo test --locked

test: test-python test-rust ## Todos os testes automatizados (rode make test-deps antes)

lint: ## clippy + rustfmt
	cd $(RS_DIR) && cargo fmt --check && cargo clippy --all-targets --locked -- -D warnings

load: ## Bateria k6 contra a stack (seed recém-aplicado). Resultados em tests/load/results
	K6="$(COMPOSE) run --rm -T k6" K6_API=http://api:8080 ./scripts/load-test.sh

clean: ## Remove containers E volumes (apaga os dados)
	$(COMPOSE) down -v
