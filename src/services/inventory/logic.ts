import type { Tx } from "../../lib/db.ts";
import { makeEvent, type Envelope } from "../../lib/events.ts";
import { addToOutbox } from "../../lib/outbox.ts";

interface Product {
  sku: string;
  price_cents: number;
  available: number;
}

/**
 * Reserve stock for an order — the flash-sale hot spot.
 *
 * "locked" (the real implementation):
 *   SELECT … FOR UPDATE locks the product rows, so concurrent reservations for the same SKU queue up
 *   and each one sees the count the previous one left. Rows are locked in SKU order, so two orders
 *   that both contain A and B always lock A first — they can't deadlock.
 *
 * "naive" (only for the load-test comparison):
 *   read, compute, write the new absolute count with no lock. Two buyers can both read "1 left",
 *   both write "0", and both get a reservation: a lost update, i.e. an oversell. The CHECK
 *   (available >= 0) never fires, because neither write goes negative.
 */
export async function reserveStock(tx: Tx, event: Envelope<"OrderPlaced">, mode: "locked" | "naive"): Promise<"reserved" | "rejected"> {
  const items = [...event.data.items].sort((a, b) => a.sku.localeCompare(b.sku));
  const skus = items.map((i) => i.sku);

  const products =
    mode === "locked"
      ? await tx<Product[]>`select sku, price_cents, available from products where sku in ${tx(skus)} order by sku for update`
      : await tx<Product[]>`select sku, price_cents, available from products where sku in ${tx(skus)} order by sku`;
  const bySku = new Map(products.map((p) => [p.sku, p]));

  const problem = items.find((i) => !bySku.has(i.sku) || bySku.get(i.sku)!.available < i.quantity);
  if (problem) {
    const reason = bySku.has(problem.sku) ? `out_of_stock: ${problem.sku}` : `unknown_sku: ${problem.sku}`;
    await addToOutbox(tx, makeEvent("StockRejected", event.orderId, { reason }));
    return "rejected";
  }

  let amountCents = 0;
  for (const item of items) {
    const p = bySku.get(item.sku)!;
    amountCents += p.price_cents * item.quantity;
    if (mode === "locked") {
      await tx`update products set available = available - ${item.quantity} where sku = ${item.sku}`;
    } else {
      // The classic bug: write back a value computed from a read that may already be stale.
      await tx`update products set available = ${p.available - item.quantity} where sku = ${item.sku}`;
    }
  }
  await tx`
    insert into reservations ${tx(items.map((i) => ({ order_id: event.orderId, sku: i.sku, quantity: i.quantity, status: "reserved" })))}
  `;
  await addToOutbox(tx, makeEvent("StockReserved", event.orderId, { amountCents, customerId: event.data.customerId }));
  return "reserved";
}

/** Compensation: payment failed, so give the units back. Safe to call twice (only 'reserved' rows). */
export async function releaseStock(tx: Tx, orderId: string): Promise<number> {
  const held = await tx<{ sku: string; quantity: number }[]>`
    select sku, quantity from reservations where order_id = ${orderId} and status = 'reserved' order by sku for update
  `;
  if (held.length === 0) return 0;
  for (const r of held) await tx`update products set available = available + ${r.quantity} where sku = ${r.sku}`;
  await tx`update reservations set status = 'released' where order_id = ${orderId} and status = 'reserved'`;
  await addToOutbox(tx, makeEvent("StockReleased", orderId, {}));
  return held.length;
}
