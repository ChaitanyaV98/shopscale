# 4. Pessimistic row locks (SELECT … FOR UPDATE) to prevent overselling

**Status:** accepted

## Context
In a flash sale thousands of buyers race for a few units. The naive code reads the count, checks
it, and writes the new count. Two buyers can both read "1 left", both pass the check and both write
"0": a **lost update**. A `CHECK (available >= 0)` constraint doesn't catch it, because no single
write goes negative.

## Decision
Reserve stock inside a transaction that locks the product rows first:

```sql
select sku, price_cents, available from products where sku in (…) order by sku for update;
-- check, then: update products set available = available - qty …
```

Concurrent reservations for the same SKU queue on the row lock, and each sees the count the
previous one left. Rows are locked **in SKU order**, so two multi-item orders can't lock A→B and
B→A and deadlock (tested). A naive mode (`RESERVATION_MODE=naive`) exists only to demonstrate the bug.

## Evidence (k6, 2,000 buyers for 100 units, same laptop)
| Mode | Confirmed orders | Oversold | Stock consistent | Placed → final, p95 |
|---|---|---|---|---|
| naive (read, then write) | **307–440** (varies per run) | **207–340** | no | 2.9 s |
| locked (`FOR UPDATE`) | **100** | **0** | yes | 3.3–4.3 s |

## Consequences
- Correct under any concurrency, any number of inventory replicas.
- **Cost:** one hot item serialises on one row, so the rush settles more slowly than the (wrong)
  naive version. Acceptable here; if one SKU's lock became the bottleneck:
  - **Atomic conditional update** — `update … set available = available - $1 where sku = $2 and available >= $1`
    in one statement, holding the lock for less time;
  - **Stock sharding** — split one SKU's stock across N rows and reserve from any shard.

## Alternatives considered
- **Optimistic concurrency** (version column, retry on conflict): great when conflicts are rare; in a
  flash sale almost every write conflicts, so it degrades into retry storms.
- **Redis `DECR` counter:** fast, but then stock lives outside the transaction that records the
  reservation and the outbox event, bringing back the dual-write problem (ADR 2).
