#!/usr/bin/env bash
# Measure peak resident memory of a single-chain index and enforce a ceiling.
#
# Runs the documented scenario - `init` USDC on mainnet, then `dev --backfill 200` against public
# RPC - samples peak RSS while it indexes, and fails if it exceeds MAX_RSS_MB. The ceiling is set
# generously (256 MB) above the measured ~37 MB so public-RPC flakiness never causes a false pass,
# and the job retries once if the first attempt indexes nothing.
#
# Env: BIN (default target/release/nuthatch), MAX_RSS_MB (default 256), PORT (default 8288).
set -euo pipefail

BIN="${BIN:-target/release/nuthatch}"
MAX_RSS_MB="${MAX_RSS_MB:-256}"
PORT="${PORT:-8288}"
USDC=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48

# Prints "<peak_rss_kb> <entities>" to stdout; all logs go to stderr.
measure() {
  local dir peak=0 rss entities=0 pid
  dir="$(mktemp -d)"
  if ! "$BIN" init "$USDC" --chain mainnet --dir "$dir" >/dev/null 2>&1; then
    echo "0 0"; return 0
  fi
  "$BIN" dev --dir "$dir" --listen "127.0.0.1:$PORT" --backfill 200 >"$dir/dev.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 40); do
    sleep 1.5
    kill -0 "$pid" 2>/dev/null || break
    rss="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ')"
    if [ -n "$rss" ] && [ "$rss" -gt "$peak" ]; then peak="$rss"; fi
    entities="$(curl -s "127.0.0.1:$PORT/" 2>/dev/null | grep -o '"entities":[0-9]*' | grep -o '[0-9]*' || true)"
    entities="${entities:-0}"
    if [ "$entities" -gt 100 ]; then break; fi
  done
  kill "$pid" 2>/dev/null || true
  echo "$peak $entities"
}

out="$(measure || echo "0 0")"
peak="${out%% *}"; entities="${out##* }"
if [ "${entities:-0}" -lt 1 ]; then
  echo "no transfers indexed (public RPC flaky?); retrying once..." >&2
  out="$(measure || echo "0 0")"
  peak="${out%% *}"; entities="${out##* }"
fi

peak_mb=$(( (${peak:-0} + 1023) / 1024 ))
echo "peak RSS: ${peak_mb} MB over ${entities:-0} transfers (ceiling ${MAX_RSS_MB} MB)"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### footprint"
    echo "peak RSS **${peak_mb} MB** over ${entities:-0} transfers (ceiling ${MAX_RSS_MB} MB)"
  } >> "$GITHUB_STEP_SUMMARY"
fi

if [ "${entities:-0}" -lt 1 ]; then
  echo "FAIL: indexed 0 transfers after retry - cannot measure; failing rather than false-passing"
  exit 1
fi
if [ "$peak_mb" -gt "$MAX_RSS_MB" ]; then
  echo "FAIL: peak RSS ${peak_mb} MB exceeds ceiling ${MAX_RSS_MB} MB"
  exit 1
fi
echo "OK: within budget"
