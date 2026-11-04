import Fastify, { type FastifyInstance } from "fastify";
import type { Sql, Tx } from "./db.ts";
import type { Envelope } from "./events.ts";
import { handleOnce } from "./inbox.ts";
import { consume, createProducer, ensureTopics, type ConsumerHandle } from "./kafka.ts";
import { startRelay, type Relay } from "./outbox.ts";

export interface ServiceContext {
  sql: Sql;
  relay: Relay;
}

export interface Subscription {
  topics: string[];
  /** Event types this service reacts to; everything else on those topics is ignored. */
  types: Envelope["type"][];
  handle: (ctx: ServiceContext, event: Envelope, tx: Tx) => Promise<void>;
}

/**
 * Everything a service needs around its business logic: HTTP server, outbox relay, idempotent Kafka
 * consumers, health check and graceful shutdown (stop taking requests → stop consuming → flush the
 * outbox → close connections), so a deploy or Ctrl+C never drops an event half-way.
 */
export async function runService(opts: {
  name: string;
  port: number;
  sql: Sql;
  routes?: (app: FastifyInstance, ctx: ServiceContext) => void;
  subscriptions?: Subscription[];
}) {
  await ensureTopics();
  const producer = await createProducer();
  const relay = startRelay(opts.sql, producer);
  const ctx: ServiceContext = { sql: opts.sql, relay };

  const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? "warn" } });
  app.get("/health", async () => {
    await opts.sql`select 1`;
    return { ok: true, service: opts.name };
  });
  opts.routes?.(app, ctx);

  const consumers: ConsumerHandle[] = [];
  for (const sub of opts.subscriptions ?? []) {
    consumers.push(
      await consume({
        groupId: `${opts.name}-svc`,
        topics: sub.topics,
        handle: async (event) => {
          if (!sub.types.includes(event.type)) return;
          const fresh = await handleOnce(opts.sql, event, (tx) => sub.handle(ctx, event, tx));
          if (fresh) relay.wake(); // publish the events this one caused right away
        },
      }),
    );
  }

  await app.listen({ port: opts.port, host: "0.0.0.0" });
  console.log(`${opts.name} listening on :${opts.port}`);

  let stopping = false;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    console.log(`${opts.name} shutting down…`);
    await app.close();
    await Promise.all(consumers.map((c) => c.stop()));
    await relay.drain().catch(() => {});
    await relay.stop();
    await producer.disconnect();
    await opts.sql.end({ timeout: 5 });
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  return { app, ctx, shutdown };
}
