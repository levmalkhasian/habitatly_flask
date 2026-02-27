#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$HOME/habitatly_flask"
APP_FILE="app.py"
PY="$APP_DIR/.venv/bin/python"

cd "$APP_DIR"

git fetch --all
git reset --hard origin/main

# Ensure venv exists
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

# Install/update deps
"$PY" -m pip install -U pip
"$PY" -m pip install -r requirements.txt

# Load env vars (so MongoDB URI exists)
set -a
source .env
set +a

# Stop previous process (match exact command)
pkill -f "$PY $APP_FILE" || true

# Start new process
nohup "$PY" "$APP_FILE" > log.txt 2>&1 &

echo "Started. Tail logs with: tail -n 200 -f $APP_DIR/log.txt"