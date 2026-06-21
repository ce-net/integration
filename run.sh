#!/usr/bin/env bash
#
# run.sh — boot a 2-node ISOLATED CE test mesh and run the cross-node assertion driver.
#
# This is the real multi-node confidence harness. It NEVER touches the live node on
# 127.0.0.1:8844: both test nodes run on unique high ports with --no-mdns (so they cannot
# cross-link the live node via LAN discovery) and on throwaway --data-dir / --ephemeral chains.
#
#   node A: api 18901, p2p 14901  (bootstrap seed)
#   node B: api 18902, p2p 14902  (--bootstrap <A multiaddr>)
#
# Both are started with: --no-mine --ephemeral --no-mdns.
# Node B is dialed at node A's constructed /ip4/127.0.0.1/tcp/<A p2p>/p2p/<A peer-id> multiaddr.
#
# Rerunnable: ports are overridable; every node + temp dir is killed/removed on exit (even on
# failure / Ctrl-C) via a trap.
#
# Usage:
#   ./run.sh                 # build the driver if needed, boot mesh, run, tear down
#   CE_BIN=/path/to/ce ./run.sh
#   A_API=18911 A_P2P=14911 B_API=18912 B_P2P=14912 ./run.sh
#
# Exit code mirrors the driver: 0 = all non-blocked scenarios passed.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CE_BIN="${CE_BIN:-/Users/07lead01/ce-net/ce/target/release/ce}"

A_API="${A_API:-18901}"
A_P2P="${A_P2P:-14901}"
B_API="${B_API:-18902}"
B_P2P="${B_P2P:-14902}"

A_DIR=""
B_DIR=""
A_PID=""
B_PID=""
A_LOG=""
B_LOG=""

log() { printf '[harness] %s\n' "$*" >&2; }
die() { printf '[harness] ERROR: %s\n' "$*" >&2; exit 1; }

cleanup() {
  local code=$?
  log "tearing down…"
  for pid in "$B_PID" "$A_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  # give them a moment, then hard-kill stragglers
  sleep 1
  for pid in "$B_PID" "$A_PID"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  for d in "$A_DIR" "$B_DIR"; do
    if [ -n "$d" ] && [ -d "$d" ]; then
      rm -rf "$d" 2>/dev/null || true
    fi
  done
  log "done (exit $code)"
}
trap cleanup EXIT INT TERM

[ -x "$CE_BIN" ] || die "ce binary not found/executable at $CE_BIN (set CE_BIN=...)"

# Guard: make sure we are not about to collide with the live node's default ports.
for p in "$A_API" "$B_API"; do
  [ "$p" = "8844" ] && die "refusing to use the live API port 8844"
done
for p in "$A_P2P" "$B_P2P"; do
  [ "$p" = "4001" ] && die "refusing to use the live P2P port 4001"
done

# Build the driver (release) up front so a compile error fails fast and cheap.
log "building integration driver…"
( cd "$HERE" && cargo build --release --quiet ) || die "driver build failed"
DRIVER="$HERE/target/release/ce-integration"
[ -x "$DRIVER" ] || die "driver binary missing at $DRIVER"

A_DIR="$(mktemp -d)"
B_DIR="$(mktemp -d)"
A_LOG="$A_DIR/node.log"
B_LOG="$B_DIR/node.log"

# wait_health <api-port> <name> <log>  — block until GET /health == 200 or time out.
wait_health() {
  local port="$1" name="$2" logf="$3"
  local url="http://127.0.0.1:${port}/health"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    # bail early if the process already died
    return_if_dead "$name" "$logf"
    sleep 0.5
  done
  log "---- $name log tail ----"; tail -n 40 "$logf" >&2 || true
  die "$name never became healthy on :$port"
}

return_if_dead() {
  local name="$1" logf="$2"
  local pid_var="${name}_PID"
  local pid="${!pid_var}"
  if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
    log "---- $name died; log tail ----"; tail -n 40 "$logf" >&2 || true
    die "$name process exited before becoming healthy"
  fi
}

read_token() {
  local dir="$1" name="$2"
  for _ in $(seq 1 40); do
    if [ -s "$dir/api.token" ]; then
      tr -d '[:space:]' < "$dir/api.token"
      return 0
    fi
    sleep 0.25
  done
  die "$name api.token never appeared in $dir"
}

# ---- boot node A (the bootstrap seed) ----
log "starting node A (api :$A_API, p2p :$A_P2P)…"
"$CE_BIN" --data-dir "$A_DIR" start \
  --no-mine --ephemeral --no-mdns \
  --api-port "$A_API" --port "$A_P2P" \
  >"$A_LOG" 2>&1 &
A_PID=$!
wait_health "$A_API" "A" "$A_LOG"
A_TOKEN="$(read_token "$A_DIR" "A")"

# Derive A's connectable multiaddr. /bootstrap returns "/p2p/<peer-id>" when no external IP is
# set; we splice in A's actual loopback listen address so B can dial it directly.
A_BOOT_RAW="$(curl -fsS "http://127.0.0.1:${A_API}/bootstrap")" || die "could not GET A /bootstrap"
A_PEER_ID="$(printf '%s' "$A_BOOT_RAW" | sed -n 's/.*\/p2p\/\([A-Za-z0-9]*\).*/\1/p' | head -n1)"
[ -n "$A_PEER_ID" ] || die "could not parse A peer id from /bootstrap: $A_BOOT_RAW"
A_MULTIADDR="/ip4/127.0.0.1/tcp/${A_P2P}/p2p/${A_PEER_ID}"
log "node A multiaddr: $A_MULTIADDR"

# ---- boot node B, bootstrapped to A ----
log "starting node B (api :$B_API, p2p :$B_P2P) bootstrapped to A…"
"$CE_BIN" --data-dir "$B_DIR" start \
  --no-mine --ephemeral --no-mdns \
  --api-port "$B_API" --port "$B_P2P" \
  --bootstrap "$A_MULTIADDR" \
  >"$B_LOG" 2>&1 &
B_PID=$!
wait_health "$B_API" "B" "$B_LOG"
B_TOKEN="$(read_token "$B_DIR" "B")"

# Mint a tunnel capability: B (the resource owner) self-issues a `tunnel` capability to A's
# NodeId, signed by B's key. The tunnel target authorizes the requester against a chain rooted
# at its own key, so this is exactly what B will honor. We grant any-port (the echo server binds
# an ephemeral port the driver chooses). `ce grant` reads B's key from its --data-dir and runs
# offline; the token is the single hex line on stdout (warnings go to stderr).
log "minting a tunnel capability: B -> A…"
A_NODE_ID="$(curl -fsS "http://127.0.0.1:${A_API}/status" | sed -n 's/.*"node_id":"\([0-9a-f]*\)".*/\1/p')"
[ -n "$A_NODE_ID" ] || die "could not read A node_id from /status"
TUNNEL_CAPS="$("$CE_BIN" --data-dir "$B_DIR" grant "$A_NODE_ID" --can tunnel 2>/dev/null | tr -d '[:space:]')"
if [ -z "$TUNNEL_CAPS" ]; then
  log "WARNING: tunnel capability mint produced no token; tunnel scenario will run uncapped (likely BLOCKED)"
fi

log "both nodes healthy; handing off to the assertion driver."

# ---- run the driver ----
set +e
CE_IT_A_BASE="http://127.0.0.1:${A_API}" \
CE_IT_A_TOKEN="$A_TOKEN" \
CE_IT_B_BASE="http://127.0.0.1:${B_API}" \
CE_IT_B_TOKEN="$B_TOKEN" \
CE_IT_TUNNEL_CAPS="${TUNNEL_CAPS:-}" \
RUST_LOG="${RUST_LOG:-info}" \
"$DRIVER"
RC=$?
set -e

if [ "$RC" -ne 0 ]; then
  log "driver exited non-zero ($RC); node A log tail:"
  tail -n 25 "$A_LOG" >&2 || true
  log "node B log tail:"
  tail -n 25 "$B_LOG" >&2 || true
fi

exit "$RC"
