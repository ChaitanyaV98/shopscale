# 2. Transactional outbox for publishing, inbox for consuming

**Status:** accepted

## Context
A service must change its database *and* tell other services. Doing both as two separate steps (a
"dual write") can't be atomic: crash between them and the database and Kafka disagree forever — an
order nobody hears about, or an event announcing a change that was rolled back.

## Decision
- **Outbox:** the event is inserted into an `outbox` table in the same transaction as the business
  change. A relay in each service publishes unpublished rows to Kafka in order, then marks them
  published. `FOR UPDATE SKIP LOCKED` lets several replicas run relays without double-publishing
  concurrently. A partial index (`where published_at is null`) keeps the relay's scan small.
- **Inbox:** the relay can publish a row twice (crash after send, before marking), and Kafka can
  redeliver, so delivery is **at-least-once**. Every consumer inserts the event id into an `inbox`
  table in the same transaction as the event's effects; a duplicate hits the primary key and does nothing.
- Kafka offsets are committed only **after** the handler's transaction commits.

Result: exactly-once *effects* on top of at-least-once *delivery*.

## Consequences
- Events are delayed by the relay's poll (≤ 200 ms; it is woken immediately after local writes).
- Outbox and inbox tables grow; production would prune published/old rows on a schedule.
- Tested: a redelivered event reserves stock once and emits one event (`consistency.int.test.ts`).

## Alternatives considered
- **Two-phase commit (XA) across Postgres and Kafka:** heavy, poorly supported, blocks on coordinator failure.
- **Change data capture (Debezium) on the WAL:** no polling, but another system to run; overkill here.
- **Kafka transactions / exactly-once semantics:** only cover Kafka-to-Kafka, not the database write.
