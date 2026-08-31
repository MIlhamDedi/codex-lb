# Change: Ratchet proxy architecture after native parity cutover

## Why

Native Codex traffic parity added bounded transport-failure classification and
shared HTTP-bridge transport-policy enforcement. The repository architecture
gate still held the pre-cutover line counts and rejected the intentional
HTTP-bridge dependency on the canonical streaming policy owner.

## What Changes

- Reset the service and streaming-mixin line ratchets to the exact measured
  post-cutover sizes; future growth remains rejected.
- Explicitly allow the HTTP-bridge domain to depend on the streaming domain for
  the single canonical transport-policy decision.

## Impact

- Affected spec: `proxy-architecture`
- Affected code: architecture fitness policy only
- Runtime behavior is unchanged.
