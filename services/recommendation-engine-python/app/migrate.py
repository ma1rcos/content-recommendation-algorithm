"""Runner de migrations SQL: aplica, em ordem, os arquivos NNNN_*.sql ainda não
registrados em schema_migrations. Usa advisory lock para que duas execuções
simultâneas não colidam.

    python -m app.migrate [--dir database/migrations]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg

LOCK_ID = 7_420_001


def migrate(dsn: str, directory: Path) -> list[str]:
    files = sorted(p for p in directory.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    applied_now: list[str] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
        try:
            conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
            done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
            for f in files:
                version = f.stem
                if version in done:
                    continue
                with conn.transaction():
                    conn.execute(f.read_text(encoding="utf-8"))
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
                applied_now.append(version)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
    return applied_now


def main() -> None:
    default_dir = os.environ.get("MIGRATIONS_DIR") or str(Path(__file__).resolve().parents[3] / "database" / "migrations")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default=default_dir)
    args = parser.parse_args()
    dsn = os.environ.get("MIGRATIONS_DATABASE_URL") or os.environ["DATABASE_URL"]
    applied = migrate(dsn, Path(args.dir))
    print(f"migrations aplicadas: {applied or 'nenhuma (já atualizado)'}")
    sys.exit(0)


if __name__ == "__main__":
    main()
