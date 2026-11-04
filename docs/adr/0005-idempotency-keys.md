# 5. Idempotency keys for orders and payments

**Status:** accepted

## Context
Networks fail after the server has done the work. A client that times out will retry; a consumer
that crashes after charging a card will be redelivered the same event. Neither may buy or charge twice.

## Decision
- **Clients → orders:** `POST /orders` requires an `Idempotency-Key` header. The key, a hash of the
  request and the order id are stored in the same transaction as the order. A retry with the same key
  returns the original order (`200`, `Idempotent-Replayed: true`); reusing a key for a *different*
  request returns `422`. Two racing requests with one key: the second waits on the primary key and then replays.
- **Payments → provider:** the order id is the idempotency key sent to the payment provider (simulated
  here with the same semantics as Stripe). If the service crashes after charging but before committing,
  the redelivered event calls the provider again with the same key and gets the original charge back.
- `payments.order_id` is also a primary key, so even a logic bug can't record two payments for one order.

## Consequences
- Clients must generate a key per purchase attempt (e.g. a UUID).
- Tested: 10 racing retries create 1 order; a crash between charge and commit charges once.

## Alternatives considered
- **De-duplicate on the request body alone:** a customer buying the same item twice on purpose would be blocked.
