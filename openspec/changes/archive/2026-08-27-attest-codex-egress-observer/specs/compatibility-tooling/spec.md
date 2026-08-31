## ADDED Requirements

### Requirement: Egress observer evidence is scoped and privacy safe

The traffic parity capture addon MUST accept an optional operator-supplied
observer id and observer role for A/C source-address comparison. It MUST retain
only a deterministic digest of the observer id and the source host observed by
the capture boundary, together with non-sensitive address-family metadata, and
MUST NOT persist the raw source host. The analyzer MUST compare source evidence
only when both paths attest the same observer id and role. Missing or
incompatible attestation MUST be reported as unobserved rather than pass or
fail. An intercept-observer match MUST NOT be described as proof of the public
source IP or ASN observed by OpenAI; that stronger claim is available only
when an actual controlled origin is explicitly declared as the observer.

#### Scenario: Same intercept observer sees the same source

- **GIVEN** Path A and Path C attest the same intercept observer
- **AND** that observer records the same digested source host for both paths
- **WHEN** a server-observable report is generated
- **THEN** the observed-source dimension passes at the intercept boundary
- **AND** the report does not claim public OpenAI source-IP or ASN parity

#### Scenario: Capture has no common observer attestation

- **GIVEN** either path omits an observer id or the attested observers differ
- **WHEN** a server-observable report is generated
- **THEN** the observed-source dimension is reported as unobserved
- **AND** absence on both paths is not treated as equality

#### Scenario: Raw addresses remain absent

- **GIVEN** the capture boundary exposes a client peer address
- **WHEN** metadata, full, or none capture mode writes a record
- **THEN** the record contains a deterministic source-host digest and address
  family
- **AND** the raw source host is absent
