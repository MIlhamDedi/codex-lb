## 1. Contract

- [x] 1.1 Specify bounded terminal transcript persistence and fail-open delivery.

## 2. Implementation

- [x] 2.1 Bound terminal drain and append in the event batcher.
- [x] 2.2 Preserve incomplete-spool settlement and cancellation cleanup on timeout.

## 3. Verification

- [x] 3.1 Add batcher regression coverage for a stalled SQLite append.
- [x] 3.2 Add proxy-path coverage proving terminal delivery precedes fallback settlement.
- [x] 3.3 Run focused tests and strict OpenSpec validation.
