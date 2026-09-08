"""Export the backend's OpenAPI spec to docs/api/openapi.json.

The committed spec is the reviewable API artifact for the final submission —
regenerate it in the same MR whenever an endpoint, model, or response contract
changes, so the file in docs/ never drifts from the code.

Usage (from the repo root, any OS — pure stdlib besides the backend deps):

    python scripts/export_openapi.py           # (re)write docs/api/openapi.json
    python scripts/export_openapi.py --check   # exit 1 if the committed spec drifted

Requires the backend dependencies (fastapi etc.) to be importable, e.g. the
backend virtualenv or the CI image that runs the backend tests.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = REPO_ROOT / "backend"
OUTPUT = REPO_ROOT / "docs" / "api" / "openapi.json"


def build_spec() -> dict:
    """Import the FastAPI app and render its OpenAPI schema (no server needed)."""
    sys.path.insert(0, str(BACKEND_DIR))
    from app.main import app  # deferred: needs backend deps + sys.path above

    return app.openapi()


def render(spec: dict) -> str:
    # Stable rendering (indent, trailing newline) so --check diffs are exact and
    # the committed file is identical regardless of the OS that generated it.
    return json.dumps(spec, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify the committed spec matches the code instead of writing it",
    )
    args = parser.parse_args(argv)

    rendered = render(build_spec())

    if args.check:
        committed = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if committed != rendered:
            print(f"OUT OF SYNC: regenerate with `python {Path(__file__).name}`", file=sys.stderr)
            return 1
        print(f"{OUTPUT.relative_to(REPO_ROOT)} is in sync with the code.")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    # open() instead of write_text(newline=...) — LF on every OS, Python 3.9+.
    with OUTPUT.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)
    print(f"Wrote {OUTPUT.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
