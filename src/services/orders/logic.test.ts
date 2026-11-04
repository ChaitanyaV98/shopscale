import { describe, expect, it } from "vitest";
import { makeEvent, type Envelope } from "../../lib/events.ts";
import { nextStatus, requestHash, type OrderStatus } from "./logic.ts";

const id = "00000000-0000-4000-8000-000000000001";
const reserved = makeEvent("StockReserved", id, { amountCents: 100, customerId: "c" });
const rejected = makeEvent("StockRejected", id, { reason: "out_of_stock: X" });
const captured = makeEvent("PaymentCaptured", id, { paymentId: id, amountCents: 100 });
const failed = makeEvent("PaymentFailed", id, { reason: "card_declined" });

describe("order state machine", () => {
  it.each<[OrderStatus, Envelope, OrderStatus | null]>([
    ["PENDING", reserved, "RESERVED"],
    ["PENDING", rejected, "CANCELLED"],
    ["RESERVED", captured, "CONFIRMED"],
    ["RESERVED", failed, "CANCELLED"],
    // Out of order: payment arrives before the reservation event (different topics).
    ["PENDING", captured, "CONFIRMED"],
    // …and the late reservation event must not drag it back.
    ["CONFIRMED", reserved, null],
    // Terminal states never change; duplicates are no-ops.
    ["CANCELLED", captured, null],
    ["CONFIRMED", failed, null],
    ["RESERVED", reserved, null],
  ])("%s + %s → %s", (from, event, to) => {
    expect(nextStatus(from, event)?.status ?? null).toBe(to);
  });

  it("keeps the reason for a cancellation", () => {
    expect(nextStatus("RESERVED", failed)).toEqual({ status: "CANCELLED", reason: "payment_failed: card_declined" });
  });
});

describe("request hash (idempotency)", () => {
  it("is the same for the same items in a different order", () => {
    const a = requestHash({ customerId: "c", items: [{ sku: "A", quantity: 1 }, { sku: "B", quantity: 2 }] });
    const b = requestHash({ customerId: "c", items: [{ sku: "B", quantity: 2 }, { sku: "A", quantity: 1 }] });
    expect(a).toBe(b);
  });

  it("differs when anything that matters changes", () => {
    const base = { customerId: "c", items: [{ sku: "A", quantity: 1 }] };
    expect(requestHash(base)).not.toBe(requestHash({ ...base, items: [{ sku: "A", quantity: 2 }] }));
    expect(requestHash(base)).not.toBe(requestHash({ ...base, customerId: "d" }));
  });
});
