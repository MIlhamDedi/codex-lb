#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC_DIR="$ROOT/spec"
JAR="$SPEC_DIR/tla2tools.jar"
TLA_VERSION="v1.7.4"
TLA_URL="https://github.com/tlaplus/tlaplus/releases/download/${TLA_VERSION}/tla2tools.jar"

download_tlc() {
  if [[ -s "$JAR" ]]; then
    return
  fi
  echo "Downloading tla2tools.jar ${TLA_VERSION} from GitHub releases..."
  curl -L --fail --retry 3 -o "$JAR" "$TLA_URL"
}

run_tlc() {
  local cfg="$1"
  local out="$2"
  java -XX:+UseParallelGC -jar "$JAR" \
    -deadlock \
    -workers auto \
    -config "$cfg" \
    "$SPEC_DIR/CoreOwnership.tla" >"$out" 2>&1
}

state_count() {
  local out="$1"
  grep -E 'states generated, [0-9,]+ distinct states found' "$out" | tail -n 1 | sed -E 's/^.*states generated, ([0-9,]+) distinct states found.*$/\1/' || true
}

expect_full_pass() {
  local out="$SPEC_DIR/.tlc-full.out"
  echo "== Full model =="
  if ! run_tlc "$SPEC_DIR/CoreOwnership.cfg" "$out"; then
    cat "$out"
    echo "Full model failed; expected zero violations and no deadlock." >&2
    exit 1
  fi
  if grep -qE 'Error:|Deadlock reached' "$out"; then
    cat "$out"
    echo "Full model reported an error/deadlock despite zero exit." >&2
    exit 1
  fi
  local distinct
  distinct="$(state_count "$out")"
  if [[ -z "$distinct" ]]; then
    cat "$out"
    echo "Could not parse full-model state count." >&2
    exit 1
  fi
  echo "PASS full: distinct states=${distinct}; zero violations; deadlock checking ON."
}

expect_weakening_fails() {
  local cfg="$1"
  local label
  label="$(basename "$cfg" .cfg)"
  local out="$SPEC_DIR/.tlc-${label}.out"
  echo "== Weakening ${label} =="
  set +e
  run_tlc "$cfg" "$out"
  local rc=$?
  set -e
  if [[ "$rc" -eq 0 ]]; then
    cat "$out"
    echo "Weakening ${label} passed; expected a TLC counterexample." >&2
    exit 1
  fi
  if ! grep -q 'Error: The behavior up to this point is:' "$out"; then
    cat "$out"
    echo "Weakening ${label} failed without a TLC counterexample trace." >&2
    exit 1
  fi
  if ! grep -qE 'Error: Invariant|Error: Temporal|Deadlock reached' "$out"; then
    cat "$out"
    echo "Weakening ${label} failed, but not through a model-checking violation." >&2
    exit 1
  fi
  local distinct
  distinct="$(state_count "$out")"
  local violation
  violation="$(grep -E 'Error: Invariant|Error: Temporal|Deadlock reached' "$out" | head -n 1)"
  echo "COUNTEREXAMPLE ${label}: ${violation}; distinct states=${distinct:-unknown}."
}

main() {
  download_tlc
  expect_full_pass

  local failures=0
  local cfg
  for cfg in "$SPEC_DIR"/weak-*.cfg; do
    expect_weakening_fails "$cfg"
    failures=$((failures + 1))
  done

  if [[ "$failures" -lt 4 ]]; then
    echo "Only ${failures} weakening counterexamples found; expected at least 4." >&2
    exit 1
  fi
  echo "PASS weakenings: ${failures} counterexample-producing negative controls."
}

main "$@"
