import type { Envelope } from "./events.ts";
import type { Sql, Tx } from "./db.ts";

/**
 * Idempotent consumer ("inbox" pattern).
 *
 * Kafka + the outbox relay deliver at-least-once, so the same event can arrive twice. Each consumer
 * records the event id in an `inbox` table in the SAME transaction as the work the event causes.
 * A second delivery hits the primary key, does nothing, and returns false: exactly-once *effects*
 * on top of at-least-once *delivery*.
 */
export async function handleOnce(sql: Sql, event: Envelope, work: (tx: Tx) => Promise<void>): Promise<boolean> {
  return sql.begin(async (tx) => {
    const [inserted] = await tx`
      insert into inbox (event_id, event_type) values (${event.id}, ${event.type})
      on conflict (event_id) do nothing
      returning event_id
    `;
    if (!inserted) return false; // already processed
    await work(tx);
    return true;
  });
}
