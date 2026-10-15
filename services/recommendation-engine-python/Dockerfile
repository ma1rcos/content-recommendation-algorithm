# syntax=docker/dockerfile:1
# Motor Python, workers, migrations e seed (mesma imagem, comandos diferentes).
# Contexto de build: raiz do repositório (precisa de database/).

FROM python:3.13-slim-bookworm AS build
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never PIP_NO_CACHE_DIR=1
RUN pip install uv==0.11.32
WORKDIR /app
COPY services/recommendation-engine-python/pyproject.toml services/recommendation-engine-python/uv.lock ./
# --frozen: falha se o lockfile não corresponder ao pyproject (dependências fixadas).
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim-bookworm AS runtime
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
COPY services/recommendation-engine-python/app ./app
COPY services/recommendation-engine-python/workers ./workers
COPY services/recommendation-engine-python/run-engine.sh ./run-engine.sh
COPY database/migrations ./database/migrations
COPY database/seeds ./database/seeds
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MIGRATIONS_DIR=/app/database/migrations \
    TAXONOMY_FILE=/app/database/seeds/taxonomy.json
USER 10001:10001
EXPOSE 8000 9100
CMD ["./run-engine.sh"]
