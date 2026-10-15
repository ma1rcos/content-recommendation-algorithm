#!/usr/bin/env python3
"""Gera tokens de demonstração aceitos pela API com AUTH_MODE=hmac.

    python3 scripts/make_token.py user 1        # token do usuário 1
    python3 scripts/make_token.py admin         # token administrativo
    AUTH_SECRET=... python3 scripts/make_token.py user 1 --ttl 3600

Formato: <tipo>.<id>.<exp>.<hex(HMAC-SHA256(AUTH_SECRET, "<tipo>.<id>.<exp>"))>
(espelha services/recommendation-api-rust/src/auth.rs). Somente stdlib.
Em produção, use um provedor de identidade (OIDC/JWT) — ver docs/ARCHITECTURE.md.
"""

import argparse
import hashlib
import hmac
import os
import sys
import time
from pathlib import Path


def secret_from_env_file() -> str | None:
    env = Path(__file__).resolve().parents[1] / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("AUTH_SECRET="):
                return line.split("=", 1)[1].strip()
    return None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("kind", choices=["user", "admin"])
    p.add_argument("id", nargs="?", type=int, default=0)
    p.add_argument("--ttl", type=int, default=24 * 3600)
    args = p.parse_args()
    secret = os.environ.get("AUTH_SECRET") or secret_from_env_file()
    if not secret:
        sys.exit("defina AUTH_SECRET (ou crie .env a partir de .env.example)")
    kind, ident = ("u", args.id) if args.kind == "user" else ("a", 0)
    if kind == "u" and ident < 1:
        sys.exit("informe o id do usuário (>= 1)")
    payload = f"{kind}.{ident}.{int(time.time()) + args.ttl}"
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    print(f"{payload}.{sig}")


if __name__ == "__main__":
    main()
