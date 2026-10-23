#!/usr/bin/env bash
# tests/integration/run.sh — ADR-0040 Tier2 (live) for the Python SDK. Drives the real
# client against the LATEST RELEASED platform: the published GHCR images, verified by the
# committed prod-signed Community license — not a source build. The kafka backend is
# ADR-0009-ungated, so it runs on a Community stack; that gives the `pos` cursor the suite
# asserts and the retention the reconnect-replay leg needs.
#
# It boots the stack with the `sukko` CLI, provisions a client fixture on the demo tenant,
# exports the SUKKO_IT_* env the conftest reads, runs the integration suite, and tears down.
#
# Requirements: docker, go (to build the CLI), uv. Provide the CLI checkout via SUKKO_CLI_DIR
# or place it at ../cli / .sources/cli (relative to the repo root).
set -euo pipefail
cd "$(dirname "$0")/../.." # repo root

# Committed prod-signed Community license — NOT a secret (the free tier unlocks nothing
# gated); it verifies against the production key embedded in the published images. Source of
# truth: the sukko platform's taskfiles/e2e.yml. NEVER commit a Pro/Enterprise key.
COMMUNITY_LICENSE_KEY="eyJlZGl0aW9uIjoiY29tbXVuaXR5Iiwib3JnIjoiS2x1cnZpbyIsImV4cCI6MjEwODExMjcyNSwiaWF0IjoxNzkyNDkzNTI2LCJsaW1pdHMiOnsiZWRpdGlvbiI6ImNvbW11bml0eSIsIm1heF90ZW5hbnRzIjozLCJtYXhfdG90YWxfY29ubmVjdGlvbnMiOjUwMCwibWF4X3NoYXJkcyI6MSwibWF4X3RvcGljc19wZXJfdGVuYW50IjoxMCwibWF4X3JvdXRpbmdfcnVsZXNfcGVyX3RlbmFudCI6MTB9fQ.e0nWF3QBfUHubE8_KvGmIfAV0jGT_ZR0hyt0qDKiMcDvNfeucuD9kuGTSey2d1wIR3o_5XP5BZWKwpenHwTQfA"

# --- 1. resolve + build the sukko CLI (fixture tool) -----------------------------------
SUKKO_CLI_DIR="${SUKKO_CLI_DIR:-}"
if [ -z "$SUKKO_CLI_DIR" ]; then
  for cand in ../cli .sources/cli; do
    if [ -f "$cand/go.mod" ]; then SUKKO_CLI_DIR="$(cd "$cand" && pwd)"; break; fi
  done
fi
if [ -z "$SUKKO_CLI_DIR" ] || [ ! -f "$SUKKO_CLI_DIR/go.mod" ]; then
  echo "ERROR: sukko CLI checkout not found — set SUKKO_CLI_DIR, or place it at ../cli or .sources/cli" >&2
  exit 1
fi
echo "==> building sukko CLI from $SUKKO_CLI_DIR"
BIN_DIR="$(mktemp -d)"
SUKKO="$BIN_DIR/sukko"
( cd "$SUKKO_CLI_DIR" && go build -o "$SUKKO" . )

# --- 2. boot the latest released Community + KAFKA stack --------------------------------
WORK="$(mktemp -d)"
export COMPOSE_PROJECT_NAME="sukko-py-e2e-$$"
cleanup() {
  if [ -n "${SUKKO_E2E_KEEP:-}" ]; then
    echo "==> SUKKO_E2E_KEEP set — leaving the stack up ($COMPOSE_PROJECT_NAME); fixture: $WORK" >&2
    return
  fi
  ( cd "$WORK" && "$SUKKO" down -v >/dev/null 2>&1 || true )
  # Belt-and-suspenders: a profile service (redpanda) under a restart policy can survive
  # `down` and leak its host port, breaking the next kafka run — force-remove any remnants.
  local leftover
  leftover="$(docker ps -aqf "name=${COMPOSE_PROJECT_NAME}" 2>/dev/null || true)"
  [ -n "$leftover" ] && docker rm -f $leftover >/dev/null 2>&1 || true
}
trap cleanup EXIT
echo "==> booting the latest released stack (published images + committed Community key, kafka backend)"
(
  cd "$WORK"
  # init prompts for the message backend then observability; answer kafka + no.
  printf 'kafka\nno\n' | "$SUKKO" init
  "$SUKKO" license set "$COMMUNITY_LICENSE_KEY"
  "$SUKKO" up --pull # pull the latest published images, not a source build
)

# --- 3. fixture: register an ES256 key for the demo tenant + mint a client JWT + api key -
echo "==> provisioning client fixture (demo tenant)"
( cd "$WORK" && "$SUKKO" keys create --generate --tenant demo )
TOKEN="$(cd "$WORK" && "$SUKKO" token generate --tenant demo --sub py-e2e 2>/dev/null | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' | head -1)"
if [ -z "$TOKEN" ]; then
  echo "ERROR: could not mint a client token" >&2
  exit 1
fi
APIKEY="$(cd "$WORK" && "$SUKKO" api-keys create --tenant demo --name py-e2e -o json 2>/dev/null | grep -oE '"key_id": *"[^"]+"' | head -1 | sed -E 's/.*"key_id": *"([^"]+)".*/\1/')"
if [ -z "$APIKEY" ]; then
  echo "ERROR: could not mint an api key" >&2
  exit 1
fi

# The conftest reads these. Single-segment suffixes so the demo tenant's `*` channel rules
# authorize both subscribe and publish.
export SUKKO_IT_WS_URL="ws://localhost:13000/ws"
export SUKKO_IT_TOKEN="$TOKEN"
export SUKKO_IT_API_KEY="$APIKEY"
export SUKKO_IT_CHANNEL="demo.itsmoke"
export SUKKO_IT_EDITION="community" # released images + the committed Community key

# --- 4. run the live integration suite --------------------------------------------------
echo "==> running the live integration suite"
uv run --extra dev pytest -m integration tests/integration "$@"
