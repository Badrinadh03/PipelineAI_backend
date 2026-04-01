#!/usr/bin/env bash
# Dev server with reload. Excludes .venv using an absolute path so the watcher
# does not restart when files under the virtualenv change (avoids reload loops).
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"
exec uvicorn main:app --reload --port 8000 --reload-exclude "${ROOT}/.venv"
