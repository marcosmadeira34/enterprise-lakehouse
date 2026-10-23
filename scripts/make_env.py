"""Create .env from .env.example, replacing every ``change-me`` value with a random secret.

Never overwrites an existing .env. Uses only the standard library (the Fernet key format is
32 url-safe base64-encoded bytes, which ``secrets`` produces directly).
"""

from __future__ import annotations

import base64
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    target = ROOT / ".env"
    if target.exists():
        print(".env already exists; leaving it untouched")
        return 0
    lines = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and value.startswith("change-me"):
            if key == "AIRFLOW_FERNET_KEY":
                value = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
            else:
                value = secrets.token_urlsafe(24)
        lines.append(f"{key}{sep}{value}" if sep else line)
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    target.chmod(0o600)
    print("created .env with random local secrets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
