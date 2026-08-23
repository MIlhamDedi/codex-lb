## Context

The bridge/service and direct WebSocket paths each slim historical
`response.create` input before upstream send. The previous façade-only repair
did not protect the direct core path and used a broad pending-call type set.

## Goals / Non-Goals

**Goals:**

- Preserve outputs needed to continue namespaced collaboration and
  multi-agent control flows.
- Make bridge and direct-WebSocket slimming equivalent.
- Retain normal payload reduction for shell and unnamespaced user tools.

**Non-Goals:**

- Do not alter namespace serialization, retry/fallback behavior, or archived
  OpenSpec history.
- Do not protect a tool merely because its name is `wait_agent` or
  `send_input`.

## Decisions

- Derive protected call IDs from historical `function_call` and
  `custom_tool_call` items whose namespace is exactly `collaboration` or
  `multi_agent_v1`; use call ID rather than tool name, because names are
  user-controlled.
- Derive those IDs from the original `ResponsesRequest.input` before the
  outbound payload normalization removes replay namespaces, then carry only
  the IDs into slimming.
- Skip slimming only for matching `function_call_output` and
  `custom_tool_call_output` items. Other output types keep their current
  treatment.
- Keep the detector in the reusable core proxy module and use it from the
  service façade, so the two live paths share the classification rule.

## Risks / Trade-offs

- [A protected result can leave an oversized request above budget] -> existing
  fail-fast `payload_too_large` handling remains authoritative.
- [Malformed or missing IDs cannot be correlated safely] -> they remain
  slimmable under the current policy.
