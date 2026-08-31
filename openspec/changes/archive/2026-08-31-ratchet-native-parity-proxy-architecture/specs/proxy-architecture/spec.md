## MODIFIED Requirements

### Requirement: Proxy architecture fitness gates are enforced

The repository SHALL ratchet `service.py` at 2606 lines and
`streaming/mixin.py` at 1119 lines after the native-parity cutover. The
HTTP-bridge domain MAY import the canonical streaming transport-policy owner;
all other cross-domain imports remain governed by the explicit allowlist.

#### Scenario: Native parity architecture is checked

- **WHEN** the repository architecture gate runs after the cutover
- **THEN** the measured files pass at their exact accepted sizes
- **AND** any later line growth fails unless a reviewed spec change advances the ratchet

#### Scenario: HTTP bridge applies canonical transport policy

- **WHEN** HTTP bridge admission evaluates the downstream transport policy
- **THEN** it may import the streaming policy owner
- **AND** it does not implement a second independent policy resolver
