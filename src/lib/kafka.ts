import kafkaPkg from "@confluentinc/kafka-javascript";
import { config } from "./config.ts";
import { parseEvent, TOPICS, type Envelope } from "./events.ts";

const { KafkaJS } = kafkaPkg;

const kafka = () =>
  new KafkaJS.Kafka({ kafkaJS: { brokers: config.kafkaBrokers, clientId: "shopscale", logLevel: KafkaJS.logLevel.ERROR } });

export interface Producer {
  send: (messages: { topic: string; key: string; value: string }[]) => Promise<void>;
  disconnect: () => Promise<void>;
}

/** Idempotent producer with acks from all in-sync replicas: a send only succeeds once it's durable. */
export async function createProducer(): Promise<Producer> {
  const producer = kafka().producer({ kafkaJS: { idempotent: true, acks: -1 } });
  await producer.connect();
  return {
    send: async (messages) => {
      const byTopic = new Map<string, { key: string; value: string }[]>();
      for (const m of messages) byTopic.set(m.topic, [...(byTopic.get(m.topic) ?? []), { key: m.key, value: m.value }]);
      await producer.sendBatch({ topicMessages: [...byTopic].map(([topic, msgs]) => ({ topic, messages: msgs })) });
    },
    disconnect: () => producer.disconnect(),
  };
}

export interface ConsumerHandle {
  stop: () => Promise<void>;
}

/**
 * Consume events and hand each one to `handle`. Offsets are committed only AFTER the handler has
 * finished (and committed its own DB transaction), so a crash means re-delivery, never loss.
 * Partitions are processed concurrently; messages within one partition (one order) stay in order.
 */
export async function consume(opts: {
  groupId: string;
  topics: string[];
  handle: (event: Envelope) => Promise<void>;
  concurrency?: number;
}): Promise<ConsumerHandle> {
  const consumer = kafka().consumer({
    kafkaJS: { groupId: opts.groupId, fromBeginning: true, autoCommit: false },
  });
  await consumer.connect();
  await consumer.subscribe({ topics: opts.topics });
  await consumer.run({
    partitionsConsumedConcurrently: opts.concurrency ?? 6,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString() ?? "";
      let event: Envelope;
      try {
        event = parseEvent(raw);
      } catch (e) {
        // A malformed message can never succeed; log it and move on instead of blocking the partition.
        console.error(`[${opts.groupId}] skipping malformed message on ${topic}/${partition}:`, (e as Error).message);
        await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
        return;
      }
      await opts.handle(event); // throws → not committed → redelivered after restart/rebalance
      await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
    },
  });
  return { stop: () => consumer.disconnect() };
}

/** Create topics with enough partitions for parallelism (idempotent). */
export async function ensureTopics(partitions = 6) {
  const admin = kafka().admin();
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const missing = Object.values(TOPICS).filter((t) => !existing.has(t));
  if (missing.length) await admin.createTopics({ topics: missing.map((topic) => ({ topic, numPartitions: partitions })) });
  await admin.disconnect();
}
