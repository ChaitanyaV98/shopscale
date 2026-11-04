# 8. Order status is a forward-only state machine

**Status:** accepted

## Context
Inventory and payment events travel on different topics, so the orders service can receive
`PaymentCaptured` before `StockReserved`. Applying events naively in arrival order would move a
confirmed order back to "reserved".

## Decision
`nextStatus(current, event)` is a pure function with one rule: an event may only move an order
**forward** (PENDING → RESERVED → CONFIRMED/CANCELLED), and terminal states never change. A late or
duplicate event is a no-op. The order row is locked while an event is applied.

## Consequences
- Correct regardless of delivery order (unit-tested, including the out-of-order cases).
- The order's total is still filled in from whichever event carries it first.
