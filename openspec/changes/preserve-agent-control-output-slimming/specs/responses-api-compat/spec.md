## ADDED Requirements

### Requirement: Namespaced agent-control tool-call outputs survive historical slimming

Every live upstream path MUST preserve a historical `function_call_output` or
`custom_tool_call_output`
unchanged before forwarding an oversized Responses `response.create` when
its non-empty `call_id` matches a historical `function_call` or
`custom_tool_call` whose namespace is exactly `collaboration` or
`multi_agent_v1`. The service MUST determine this from the historical prefix of
the original request input before outbound payload normalization removes replay
namespaces, and use namespace and call ID rather than the tool name alone. A
recent namespaced call MUST NOT protect a historical output that reuses its
call ID. When historical calls of the same protocol reuse one call ID, the
service MUST pair outputs to calls by per-protocol occurrence: the nth
matching output is preserved only when the nth matching call is namespaced.
Historical outputs
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

#### Scenario: Namespaced custom tool output is retained after wire normalization
- **WHEN** a historical `collaboration` `custom_tool_call` has a large
  matching `custom_tool_call_output`, and another custom call uses a namespace
  outside the agent-control allowlist
- **THEN** HTTP bridge and WebSocket bridge forwarding preserve the
  agent-control custom output even though both outbound payloads omit replay
  namespaces
- **AND** the unrelated custom output remains eligible for the historical
  tool-output omission policy

#### Scenario: Recent calls do not protect reused historical IDs
- **WHEN** a historical unrelated output reuses the call ID of an
  agent-control call that appears only after the latest user item
- **THEN** every live slimming path leaves the historical output eligible for
  the normal omission policy

#### Scenario: Same-protocol reused call IDs pair by occurrence
- **WHEN** a historical namespaced `function_call` and an ordinary
  `function_call` reuse one call ID, each followed by a large matching
  `function_call_output`
- **THEN** both the bridge/service and direct WebSocket paths preserve the
  namespaced pair's output unchanged
- **AND** both paths replace the ordinary pair's output with the historical
  tool-output omission notice
