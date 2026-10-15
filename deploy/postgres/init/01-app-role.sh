#!/bin/sh
# Executado UMA vez, na criação do volume do PostgreSQL (docker-entrypoint-initdb.d).
# Cria a role de aplicação com privilégio mínimo; os GRANTs ficam na migration
# 0001 (aplicada pelo serviço "migrate" com o usuário dono do banco).
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_password="$APP_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE reco_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reco_app') \gexec
REVOKE ALL ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO reco_app;
SQL
