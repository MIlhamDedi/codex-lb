## 1. Implementation

- [x] 1.1 Classify protected agent-control call IDs by exact namespace and
      `function_call` type in the reusable proxy slimming logic.
- [x] 1.2 Apply that classification in both the bridge/service and direct
      WebSocket historical slimming loops.

## 2. Regression coverage

- [x] 2.1 Prove both live slim paths retain a namespaced agent wait output
      while slimming an unrelated shell output and an unnamespaced bare-name
      user tool.

## 3. Validation

- [x] 3.1 Run the focused unit test and OpenSpec validation.
