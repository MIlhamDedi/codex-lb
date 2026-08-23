## 1. Implementation

- [x] 1.1 Classify protected agent-control call IDs by exact namespace and
      `function_call` or `custom_tool_call` type in the reusable proxy
      slimming logic.
- [x] 1.2 Apply that classification in both the bridge/service and direct
      WebSocket historical slimming loops.
- [x] 1.3 Classify IDs from the original request before outbound normalization
      strips replay namespaces, while keeping the normalized wire payload.

## 2. Regression coverage

- [x] 2.1 Prove both live slim paths retain a namespaced agent wait output
      while slimming an unrelated shell output and an unnamespaced bare-name
      user tool.
- [x] 2.2 Prove HTTP and WebSocket bridge forwarding preserves both namespaced
      function and custom outputs after wire namespace stripping, while an
      unrelated namespaced custom output is still slimmed.

## 3. Validation

- [x] 3.1 Run the focused unit test and OpenSpec validation.
