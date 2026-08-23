## ADDED Requirements

### Requirement: Namespaced agent-control function-call outputs survive historical slimming

Every live upstream path MUST preserve a historical `function_call_output`
unchanged before forwarding an oversized Responses `response.create` when
its non-empty `call_id` matches a historical `function_call` whose namespace is
exactly `collaboration` or `multi_agent_v1`. The service MUST determine this
from namespace and call ID, not from the tool name alone. Historical outputs
without such a matching call, including an unnamespaced user tool named
`wait_agent` or `send_input`, MUST remain eligible for the normal omission
policy.

#### Scenario: Agent wait output is retained while unrelated outputs are slimmed
- **WHEN** a historical `multi_agent_v1` `function_call` for `wait_agent` has
  a large matching `function_call_output` and the request also has a large
  shell output before the latest user turn
- **THEN** both the bridge/service and direct WebSocket paths preserve the
  agent wait output unchanged
- **AND** both paths replace the shell output with the historical tool-output
  omission notice

#### Scenario: A bare-name user tool is not exempt
- **WHEN** a historical unnamespaced `function_call` is named `wait_agent` or
  `send_input` and has a large matching `function_call_output`
- **THEN** each live slimming path leaves that output eligible for the normal
  historical tool-output omission policy
