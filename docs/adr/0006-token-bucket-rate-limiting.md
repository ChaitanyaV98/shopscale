# 6. Token-bucket rate limiting in Redis, failing open

**Status:** accepted

## Context
Flash sales attract bots. Without limits, a few clients can take most of the capacity (and the stock).

## Decision
A token bucket per client in Redis, implemented as a Lua script so refill-check-take is one atomic
step (concurrent requests can't both take the last token — tested with 100 requests on a 10-token
bucket: exactly 10 admitted). It uses Redis's clock (`TIME`), so gateway replicas with drifting
clocks agree. Writes: 5/s, burst 10. Reads: 50/s, burst 100. Responses carry `RateLimit-*` and `Retry-After`.

Clients are keyed by customer id. In production that comes from a verified auth token; this demo has
no auth, so it is simulated with an `X-Customer-Id` header (falling back to the IP).

**If Redis is down, the gateway fails open** (requests pass, a warning is logged). The limiter
protects capacity, not correctness — row locks (ADR 4) prevent overselling either way — so a Redis
outage shouldn't close the shop. (Verified by stopping Redis mid-test.)

## Evidence
In the k6 run, 5 bots sending 100 requests/s had ~70% of requests refused (699 of 994), while all
2,000 real buyers were accepted.

## Consequences
- One Redis round trip per request (~sub-millisecond locally).
- Failing open means no protection during a Redis outage; a stricter system (e.g. paid API quotas)
  might fail closed instead.

## Alternatives considered
- **Fixed window counter:** simpler, but allows 2× the limit across a window boundary.
- **In-memory limiter per gateway:** no Redis needed, but limits multiply with the number of replicas.
