# syntax=docker/dockerfile:1
# API Rust — build multi-stage. A imagem final contém só o binário (sem toolchain).

FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
# 1) Dependências em camada própria: só recompilam quando Cargo.toml/lock mudam.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
 && cargo build --release --locked \
 && rm -rf src target/release/.fingerprint/recommendation-api-* target/release/deps/recommendation_api-*
# 2) Código da aplicação.
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --no-create-home app
COPY --from=build /src/target/release/recommendation-api /usr/local/bin/recommendation-api
USER 10001:10001
EXPOSE 8080
ENV BIND_ADDR=0.0.0.0:8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["recommendation-api", "healthcheck"]
ENTRYPOINT ["recommendation-api"]
