import type { Envelope } from "./events.ts";
import { TOPIC_OF } from "./events.ts";
import type { Sql, Tx } from "./db.ts";
import type { Producer } from "./kafka.ts";

/**
 * Transactional outbox.
 *
 * The problem: a service must change its database AND tell other services. Writing to the DB and
 * then publishing to Kafka can't be one atomic step — crash in between and the two disagree forever
 * (an order exists that nobody hears about, or an event announces a change that was rolled back).
 *
 * The fix: write the event into an `outbox` table in the SAME transaction as the business change.
 * Either both are saved or neither is. A relay then publishes outbox rows to Kafka and marks them
 * sent. If the relay crashes after publishing but before marking, the event is published again on
 * restart — so delivery is at-least-once, and every consumer de-duplicates by event id (see inbox.ts).
 */
export async function addToOutbox(tx: Tx, event: Envelope) {
  await tx`
    insert into outbox (id, topic, key, payload)
    values (${event.id}, ${TOPIC_OF[event.type]}, ${event.orderId}, ${tx.json(event as never)})
  `;
}

export interface Relay {
  wake: () => void;
  stop: () => Promise<void>;
  /** Publish everything currently pending, then resolve (used by tests). */
  drain: () => Promise<void>;
}

/**
 * Publishes outbox rows in id order, in batches. `FOR UPDATE SKIP LOCKED` lets several replicas of
 * a service run relays at once without publishing the same row twice concurrently.
 */
export function startRelay(sql: Sql, producer: Producer, opts: { pollMs?: number; batch?: number } = {}): Relay {
  const pollMs = opts.pollMs ?? 200;
  const batch = opts.batch ?? 200;
  let stopped = false;
  let wakeUp: (() => void) | null = null;

  const publishBatch = async (): Promise<number> =>
    sql.begin(async (tx) => {
      const rows = await tx<{ id: string; topic: string; key: string; payload: Envelope }[]>`
        select id, topic, key, payload from outbox
        where published_at is null
        order by seq
        limit ${batch}
        for update skip locked
      `;
      if (rows.length === 0) return 0;
      await producer.send(rows.map((r) => ({ topic: r.topic, key: r.key, value: JSON.stringify(r.payload) })));
      await tx`update outbox set published_at = now() where id in ${tx(rows.map((r) => r.id))}`;
      return rows.length;
    });

  const drain = async () => {
    while ((await publishBatch()) > 0);
  };

  const loop = (async () => {
    while (!stopped) {
      try {
        if ((await publishBatch()) > 0) continue; // more may be waiting
      } catch (e) {
        console.error("[outbox] publish failed, will retry:", (e as Error).message);
      }
      await new Promise<void>((resolve) => {
        wakeUp = resolve;
        setTimeout(resolve, pollMs);
      });
      wakeUp = null;
    }
  })();

  return {
    wake: () => wakeUp?.(),
    drain,
    stop: async () => {
      stopped = true;
      wakeUp?.();
      await loop;
    },
  };
}
