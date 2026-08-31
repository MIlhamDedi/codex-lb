## ADDED Requirements

### Requirement: Terminal transcript persistence has a live-delivery bound

The HTTP bridge MUST apply a finite application-level bound to draining and
appending optional transcript data for a terminal upstream event. If the bound
expires, the proxy MUST keep the event spool incomplete, MUST queue the selected
terminal event and end-of-stream marker without waiting for transcript
persistence, and MUST attempt the existing owner-fenced terminal settlement.
The bounded failure MUST NOT make a partial or uncertain transcript replayable.
A terminal append that finishes within the bound MUST preserve the existing
atomic terminal-state and replay behavior.

#### Scenario: Busy transcript writer does not hold live completion

- **GIVEN** an acknowledged HTTP-bridge operation has selected a terminal event
- **AND** its transcript drain or terminal append does not finish within the bound
- **WHEN** the persistence bound expires
- **THEN** the terminal event and end-of-stream marker are queued
- **AND** fallback settlement keeps the event spool incomplete
- **AND** reconnect recovery does not replay the partial transcript

#### Scenario: Timely terminal append remains replayable

- **WHEN** terminal transcript drain and append finish within the bound
- **THEN** the terminal event and intended operation state are persisted atomically
- **AND** the completed event spool remains eligible for replay
