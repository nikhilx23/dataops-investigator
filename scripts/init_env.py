"""Create a local .env from .env.example with freshly generated development passwords.

Usage: python scripts/init_env.py [--force]

Refuses to overwrite an existing .env unless --force is given. Prints only the names of
the variables it generated, never their values.
"""

from __future__ import annotations

import contextlib
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str]) -> int:
    target = ROOT / ".env"
    if target.exists() and "--force" not in argv:
        print(f"{target} already exists; pass --force to regenerate it.", file=sys.stderr)
        return 1
    generated: list[str] = []
    lines_out: list[str] = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        key, sep, rest = line.partition("=")
        if (
            sep
            and not line.lstrip().startswith("#")
            and rest.split("#", 1)[0].strip() == "CHANGE_ME"
        ):
            lines_out.append(f"{key}={secrets.token_urlsafe(18)}")
            generated.append(key)
        else:
            lines_out.append(line)
    target.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
    with contextlib.suppress(OSError):  # chmod is a no-op on some Windows filesystems
        target.chmod(0o600)
    print(f"Wrote {target} with generated values for: {', '.join(generated)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
