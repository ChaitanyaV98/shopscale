# 7. Redpanda locally, Confluent's Kafka client

**Status:** accepted

## Context
The services need a durable, partitioned log. Kafka + ZooKeeper (or KRaft) is heavy on an 8 GB laptop.

## Decision
- **Redpanda** in Docker: Kafka-API compatible, a single small container. The code uses only the
  Kafka protocol, so production could use Kafka, MSK or Confluent Cloud unchanged.
- **`@confluentinc/kafka-javascript`** (Confluent's official client, librdkafka-based). KafkaJS has
  not been updated since 2023.
- Topics: one per producing service, 6 partitions each. The **order id is the message key**, so all
  events for one order land on one partition and are consumed in order; different orders are
  processed in parallel.
- Producer: idempotent, `acks=all`.

## Consequences
- Ordering is per order *within a topic*, not across topics — which is why order status is a
  forward-only state machine (ADR 8).
