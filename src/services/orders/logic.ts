import { createHash, randomUUID } from "node:crypto";
import postgres from "postgres";
import type { Sql, Tx } from "../../lib/db.ts";
import { makeEvent, type Envelope } from "../../lib/events.ts";
import { addToOutbox } from "../../lib/outbox.ts";

export type OrderStatus = "PENDING" | "RESERVED" | "CONFIRMED" | "CANCELLED";

export interface PlaceOrderInput {
  idempotencyKey: string;
  customerId: string;
  items: { sku: string; quantity: number }[];
}

export type PlaceOrderResult =
  | { kind: "created" | "replayed"; orderId: string; status: OrderStatus }
  | { kind: "key_reused"; orderId: string };

/** Same request → same hash, regardless of item order. */
export function requestHash(input: Omit<PlaceOrderInput, "idempotencyKey">): string {
  const items = [...input.items].sort((a, b) => a.sku.localeCompare(b.sku));
  return createHash("sha256").update(JSON.stringify({ c: input.customerId, items })).digest("hex");
}

/**
 * Create an order and its OrderPlaced event atomically, guarded by an idempotency key.
 *
 * Retrying with the same key returns the original order ("replayed"), so a client that timed out
 * can safely retry without buying twice. Two requests racing with the same key: the second one
 * waits on the primary-key index until the first commits, then sees the conflict and replays.
 */
export async function placeOrder(sql: Sql, input: PlaceOrderInput): Promise<PlaceOrderResult> {
  const hash = requestHash(input);
  const orderId = randomUUID();
  try {
    await sql.begin(async (tx) => {
      await tx`insert into orders (id, customer_id, status) values (${orderId}, ${input.customerId}, 'PENDING')`;
      await tx`insert into order_items ${tx(input.items.map((i) => ({ order_id: orderId, sku: i.sku, quantity: i.quantity })))}`;
      await tx`insert into idempotency_keys (key, request_hash, order_id) values (${input.idempotencyKey}, ${hash}, ${orderId})`;
      await addToOutbox(tx, makeEvent("OrderPlaced", orderId, { customerId: input.customerId, items: input.items }));
    });
    return { kind: "created", orderId, status: "PENDING" };
  } catch (e) {
    if (!(e instanceof postgres.PostgresError && e.code === "23505" && e.constraint_name === "idempotency_keys_pkey")) throw e;
    const [existing] = await sql<{ request_hash: string; order_id: string; status: OrderStatus }[]>`
      select k.request_hash, k.order_id, o.status
      from idempotency_keys k join orders o on o.id = k.order_id
      where k.key = ${input.idempotencyKey}
    `;
    if (!existing) throw e;
    if (existing.request_hash !== hash) return { kind: "key_reused", orderId: existing.order_id };
    return { kind: "replayed", orderId: existing.order_id, status: existing.status };
  }
}

export async function getOrder(sql: Sql, id: string) {
  const [order] = await sql`
    select id, customer_id, status, cancel_reason, total_cents, created_at, updated_at from orders where id = ${id}
  `;
  if (!order) return null;
  const items = await sql`select sku, quantity from order_items where order_id = ${id} order by sku`;
  return { ...order, items };
}

const RANK: Record<OrderStatus, number> = { PENDING: 0, RESERVED: 1, CONFIRMED: 2, CANCELLED: 2 };

/**
 * The order state machine, as a pure function.
 *
 * Events come from different topics (inventory, payments), so they can arrive in any order — e.g.
 * PaymentCaptured before StockReserved. Rule: a transition may only move an order FORWARD, and
 * terminal states (CONFIRMED, CANCELLED) never change. A late or duplicate event is a no-op.
 */
export function nextStatus(current: OrderStatus, event: Envelope): { status: OrderStatus; reason?: string } | null {
  const target: { status: OrderStatus; reason?: string } | null =
    event.type === "StockReserved"
      ? { status: "RESERVED" }
      : event.type === "StockRejected"
        ? { status: "CANCELLED", reason: (event as Envelope<"StockRejected">).data.reason }
        : event.type === "PaymentCaptured"
          ? { status: "CONFIRMED" }
          : event.type === "PaymentFailed"
            ? { status: "CANCELLED", reason: `payment_failed: ${(event as Envelope<"PaymentFailed">).data.reason}` }
            : null;
  if (!target) return null;
  return RANK[target.status] > RANK[current] ? target : null;
}

/** Apply an inventory/payment event to its order, inside the inbox transaction. */
export async function applyEvent(tx: Tx, event: Envelope): Promise<void> {
  // Lock the order row so two events for the same order can't race each other.
  const [order] = await tx<{ status: OrderStatus }[]>`select status from orders where id = ${event.orderId} for update`;
  if (!order) return; // not ours (shouldn't happen); nothing to do
  const next = nextStatus(order.status, event);
  const amount =
    event.type === "StockReserved" || event.type === "PaymentCaptured"
      ? (event as Envelope<"StockReserved">).data.amountCents
      : null;
  if (next) {
    await tx`
      update orders set status = ${next.status}, cancel_reason = ${next.reason ?? null},
        total_cents = coalesce(${amount}, total_cents), updated_at = now()
      where id = ${event.orderId}
    `;
  } else if (amount !== null) {
    await tx`update orders set total_cents = coalesce(total_cents, ${amount}) where id = ${event.orderId}`;
  }
}
