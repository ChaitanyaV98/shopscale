import { randomUUID } from "node:crypto";
import type { Sql, Tx } from "../../lib/db.ts";
import { makeEvent, type Envelope } from "../../lib/events.ts";
import { addToOutbox } from "../../lib/outbox.ts";

/**
 * A stand-in for an external payment provider (think Stripe). Like a real one, it de-duplicates on
 * the Idempotency-Key we send: the same key returns the original charge instead of charging again.
 *
 * Declines: customers whose id starts with "declined" are always declined (for tests and demos), plus
 * an optional random decline rate (PAYMENT_DECLINE_RATE) for load tests.
 */
export async function providerCharge(sql: Sql, idempotencyKey: string, amountCents: number, customerId: string) {
  const declineRate = Number(process.env.PAYMENT_DECLINE_RATE ?? 0);
  const succeeded = !customerId.startsWith("declined") && Math.random() >= declineRate;
  const [created] = await sql<{ charge_id: string; succeeded: boolean }[]>`
    insert into provider_charges (idempotency_key, charge_id, amount_cents, succeeded)
    values (${idempotencyKey}, ${randomUUID()}, ${amountCents}, ${succeeded})
    on conflict (idempotency_key) do nothing
    returning charge_id, succeeded
  `;
  if (created) return { chargeId: created.charge_id, succeeded: created.succeeded, replayed: false };
  const [existing] = await sql<{ charge_id: string; succeeded: boolean }[]>`
    select charge_id, succeeded from provider_charges where idempotency_key = ${idempotencyKey}
  `;
  return { chargeId: existing!.charge_id, succeeded: existing!.succeeded, replayed: true };
}

export type Charge = Awaited<ReturnType<typeof providerCharge>>;

/**
 * Step 1, OUTSIDE any database transaction: ask the provider to charge.
 *
 * Holding a transaction open across a network call is a classic mistake: the connection sits idle
 * for the whole call, and if the call needs another connection from the same pool, a small pool
 * deadlocks (this happened with a 4-connection pool and 6 concurrent handlers). The order id is the
 * idempotency key, so calling again after a crash or redelivery returns the original charge.
 */
export function chargeForOrder(sql: Sql, event: Envelope<"StockReserved">): Promise<Charge> {
  return providerCharge(sql, `order:${event.orderId}`, event.data.amountCents, event.data.customerId);
}

/** Step 2, inside the inbox transaction: record the outcome and emit the event. */
export async function recordPayment(tx: Tx, event: Envelope<"StockReserved">, charge: Charge) {
  const status = charge.succeeded ? "captured" : "failed";
  const [row] = await tx`
    insert into payments (order_id, payment_id, amount_cents, status, failure_reason)
    values (${event.orderId}, ${charge.chargeId}, ${event.data.amountCents}, ${status}, ${charge.succeeded ? null : "card_declined"})
    on conflict (order_id) do nothing
    returning order_id
  `;
  if (!row) return; // already recorded (belt and braces on top of the inbox)
  await addToOutbox(
    tx,
    charge.succeeded
      ? makeEvent("PaymentCaptured", event.orderId, { paymentId: charge.chargeId, amountCents: event.data.amountCents })
      : makeEvent("PaymentFailed", event.orderId, { reason: "card_declined" }),
  );
}
