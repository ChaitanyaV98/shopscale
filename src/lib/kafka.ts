import kafkaPkg from "@confluentinc/kafka-javascript";
import { config } from "./config.ts";
import { parseEvent, TOPICS, type Envelope } from "./events.ts";

const { KafkaJS } = kafkaPkg;

/**
 * Connection settings. Locally: plain TCP to Redpanda. Hosted Kafka (e.g. Aiven) needs TLS plus either
 * SASL/SCRAM credentials or a client certificate — all from env vars, PEM contents passed inline.
 */
function kafkaConfig() {
  const env = process.env;
  const tls = env.KAFKA_SSL === "true" || Boolean(env.KAFKA_SSL_CA);
  const sasl = env.KAFKA_SASL_USERNAME
    ? {
        mechanism: (env.KAFKA_SASL_MECHANISM ?? "scram-sha-256") as "scram-sha-256",
        username: env.KAFKA_SASL_USERNAME,
        password: env.KAFKA_SASL_PASSWORD ?? "",
      }
    : undefined;
  return {
    kafkaJS: {
      brokers: config.kafkaBrokers,
      clientId: "shopscale",
      logLevel: KafkaJS.logLevel.ERROR,
      ssl: tls,
      ...(sasl ? { sasl } : {}),
    },
    // librdkafka settings (outside the kafkaJS block): CA and optional client certificate.
    ...(env.KAFKA_SSL_CA ? { "ssl.ca.pem": env.KAFKA_SSL_CA } : {}),
    ...(env.KAFKA_SSL_CERT ? { "ssl.certificate.pem": env.KAFKA_SSL_CERT, "ssl.key.pem": env.KAFKA_SSL_KEY ?? "" } : {}),
  };
}

const kafka = () => new KafkaJS.Kafka(kafkaConfig() as ConstructorParameters<typeof KafkaJS.Kafka>[0]);

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
export async function ensureTopics(partitions = Number(process.env.KAFKA_PARTITIONS ?? 6)) {
  const admin = kafka().admin();
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const missing = Object.values(TOPICS).filter((t) => !existing.has(t));
  if (missing.length) await admin.createTopics({ topics: missing.map((topic) => ({ topic, numPartitions: partitions })) });
  await admin.disconnect();
}
