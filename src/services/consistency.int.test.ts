/**
 * Consistency guarantees, tested against the real databases (no Kafka needed: we call the same
 * handlers the consumers call, which is where the guarantees live).
 */
import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { connect } from "../lib/db.ts";
import { makeEvent, type Envelope } from "../lib/events.ts";
import { handleOnce } from "../lib/inbox.ts";
import { releaseStock, reserveStock } from "./inventory/logic.ts";
import { applyEvent, getOrder, placeOrder } from "./orders/logic.ts";
import { chargeForOrder, recordPayment } from "./payments/logic.ts";

const orders = connect("orders");
const inventory = connect("inventory");
const payments = connect("payments");
const SKU = "TEST-FLASH";

beforeEach(async () => {
  await inventory`delete from reservations where sku = ${SKU}`;
  await inventory`insert into products (sku, name, price_cents, available) values (${SKU}, 'Test item', 1000, 10)
                  on conflict (sku) do update set available = 10`;
});
afterAll(async () => {
  await inventory`delete from reservations where sku = ${SKU}`;
  await inventory`delete from products where sku = ${SKU}`;
  await Promise.all([orders.end(), inventory.end(), payments.end()]);
});

const orderPlaced = (qty = 1) =>
  makeEvent("OrderPlaced", randomUUID(), { customerId: `c-${randomUUID().slice(0, 6)}`, items: [{ sku: SKU, quantity: qty }] });
const stock = async () => (await inventory<{ available: number }[]>`select available from products where sku = ${SKU}`)[0]!.available;
const reservedUnits = async () =>
  (await inventory<{ n: number }[]>`select coalesce(sum(quantity), 0)::int as n from reservations where sku = ${SKU} and status = 'reserved'`)[0]!.n;
const outboxTypes = async (orderId: string) =>
  (await inventory<{ type: string }[]>`select payload->>'type' as type from outbox where key = ${orderId} order by seq`).map((r) => r.type);

describe("no overselling", () => {
  it("50 concurrent buyers for 10 units: exactly 10 reserved (SELECT … FOR UPDATE)", async () => {
    const events = Array.from({ length: 50 }, () => orderPlaced());
    const results = await Promise.all(events.map((e) => handleOnce(inventory, e, (tx) => reserveStock(tx, e as Envelope<"OrderPlaced">, "locked").then(() => {}))));

    expect(results.every(Boolean)).toBe(true);
    expect(await reservedUnits()).toBe(10);
    expect(await stock()).toBe(0);
  });

  it("orders with several SKUs locked in opposite orders don't deadlock", async () => {
    const SKU2 = "TEST-FLASH-2";
    await inventory`insert into products (sku, name, price_cents, available) values (${SKU2}, 'Test 2', 500, 100) on conflict (sku) do update set available = 100`;
    const mk = (a: string, b: string) =>
      makeEvent("OrderPlaced", randomUUID(), { customerId: "c", items: [{ sku: a, quantity: 1 }, { sku: b, quantity: 1 }] });
    const events = Array.from({ length: 20 }, (_, i) => (i % 2 ? mk(SKU, SKU2) : mk(SKU2, SKU)));
    await Promise.all(events.map((e) => handleOnce(inventory, e, (tx) => reserveStock(tx, e as Envelope<"OrderPlaced">, "locked").then(() => {}))));
    expect(await reservedUnits()).toBe(10); // SKU had 10 units
    await inventory`delete from reservations where sku = ${SKU2}`;
    await inventory`delete from products where sku = ${SKU2}`;
  });
});

describe("exactly-once effects on at-least-once delivery", () => {
  it("a redelivered OrderPlaced reserves stock once and emits one event", async () => {
    const e = orderPlaced(3);
    const run = () => handleOnce(inventory, e, (tx) => reserveStock(tx, e as Envelope<"OrderPlaced">, "locked").then(() => {}));
    expect(await run()).toBe(true);
    expect(await run()).toBe(false); // duplicate delivery: no effect
    expect(await stock()).toBe(7);
    expect(await outboxTypes(e.orderId)).toEqual(["StockReserved"]);
  });

  it("out-of-stock orders are rejected with a reason", async () => {
    const e = orderPlaced(11);
    await handleOnce(inventory, e, (tx) => reserveStock(tx, e as Envelope<"OrderPlaced">, "locked").then(() => {}));
    expect(await outboxTypes(e.orderId)).toEqual(["StockRejected"]);
    expect(await stock()).toBe(10);
  });

  it("compensation releases stock once, even if PaymentFailed arrives twice", async () => {
    const e = orderPlaced(4);
    await handleOnce(inventory, e, (tx) => reserveStock(tx, e as Envelope<"OrderPlaced">, "locked").then(() => {}));
    expect(await stock()).toBe(6);
    await inventory.begin((tx) => releaseStock(tx, e.orderId));
    await inventory.begin((tx) => releaseStock(tx, e.orderId)); // second time: nothing left to release
    expect(await stock()).toBe(10);
    expect(await outboxTypes(e.orderId)).toEqual(["StockReserved", "StockReleased"]);
  });
});

describe("payments", () => {
  it("charging the same order twice charges the provider once", async () => {
    const orderId = randomUUID();
    const e = makeEvent("StockReserved", orderId, { amountCents: 2500, customerId: "alice" });
    // Simulate a crash after the provider call: the first transaction rolls back, the event is redelivered.
    const ev = e as Envelope<"StockReserved">;
    const first = await chargeForOrder(payments, ev);
    await payments
      .begin(async (tx) => {
        await recordPayment(tx, ev, first);
        throw new Error("crash before commit");
      })
      .catch(() => {});
    const retry = await chargeForOrder(payments, ev); // redelivery: same idempotency key
    expect(retry.chargeId).toBe(first.chargeId);
    expect(retry.replayed).toBe(true);
    await payments.begin((tx) => recordPayment(tx, ev, retry));

    const charges = await payments`select count(*)::int as n from provider_charges where idempotency_key = ${`order:${orderId}`}`;
    const rows = await payments`select status from payments where order_id = ${orderId}`;
    expect(charges[0]!.n).toBe(1);
    expect(rows.map((r) => r.status)).toEqual(["captured"]);
  });
});

describe("connection pool", () => {
  it("payments doesn't deadlock a tiny pool: 12 concurrent charges on 2 connections", async () => {
    // Regression: charging used to hold a transaction while asking the pool for a second
    // connection; with more concurrent handlers than connections, everything froze.
    const tiny = connect("payments", 2);
    const events = Array.from({ length: 12 }, () =>
      makeEvent("StockReserved", randomUUID(), { amountCents: 100, customerId: "pool" }) as Envelope<"StockReserved">,
    );
    const work = Promise.all(
      events.map(async (ev) => {
        const charge = await chargeForOrder(tiny, ev);
        await handleOnce(tiny, ev, (tx) => recordPayment(tx, ev, charge));
      }),
    );
    const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error("deadlocked")), 10_000));
    await Promise.race([work, timeout]);
    const rows = await tiny`select count(*)::int as n from payments where order_id in ${tiny(events.map((e) => e.orderId))}`;
    expect(rows[0]!.n).toBe(12);
    await tiny.end();
  });
});

describe("orders", () => {
  it("same Idempotency-Key: one order, even when 10 retries race", async () => {
    const key = randomUUID();
    const input = { idempotencyKey: key, customerId: "racer", items: [{ sku: "MUG", quantity: 1 }] };
    const results = await Promise.all(Array.from({ length: 10 }, () => placeOrder(orders, input)));
    const ids = new Set(results.map((r) => r.orderId));
    expect(ids.size).toBe(1);
    expect(results.filter((r) => r.kind === "created")).toHaveLength(1);
    expect(results.filter((r) => r.kind === "replayed")).toHaveLength(9);
  });

  it("events applied out of order still end in the right state", async () => {
    const { orderId } = await placeOrder(orders, { idempotencyKey: randomUUID(), customerId: "x", items: [{ sku: "MUG", quantity: 1 }] });
    const captured = makeEvent("PaymentCaptured", orderId, { paymentId: randomUUID(), amountCents: 299 });
    const reserved = makeEvent("StockReserved", orderId, { amountCents: 299, customerId: "x" });
    await handleOnce(orders, captured, (tx) => applyEvent(tx, captured));
    await handleOnce(orders, reserved, (tx) => applyEvent(tx, reserved)); // late
    const order = await getOrder(orders, orderId);
    expect(order?.status).toBe("CONFIRMED");
    expect(order?.total_cents).toBe(299);
  });
});
