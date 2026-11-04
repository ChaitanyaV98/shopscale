# 3. Choreographed saga with compensation

**Status:** accepted

## Context
Placing an order touches three services that can't share a transaction (ADR 1). If payment fails
after stock was reserved, the reservation must be undone.

## Decision
A choreographed saga: each service reacts to events and emits its own, with no central coordinator.

```
OrderPlaced ─► inventory reserves ─► StockReserved ─► payments charges ─► PaymentCaptured ─► order CONFIRMED
                     └─► StockRejected ─► order CANCELLED          └─► PaymentFailed ─► inventory releases stock (compensation)
                                                                                     └─► order CANCELLED
```

Compensation (`releaseStock`) only touches rows still `reserved`, so running it twice is harmless.

## Consequences
- Simple with three steps; the flow is readable from the event types alone.
- Harder to see "where is order X?" at a glance than with an orchestrator — the orders service's
  status (ADR 8) is the answer.
- With many more steps, an orchestrator would be easier to reason about.

## Alternatives considered
- **Orchestrator** (a service or workflow engine that calls each step): one place for the logic and
  timeouts, but a central dependency and more code for three steps.
