import { describe, expect, it } from "vitest";
import { makeEvent, parseEvent, TOPIC_OF, TOPICS } from "./events.ts";

describe("events", () => {
  const orderId = "00000000-0000-4000-8000-000000000001";

  it("round-trips through JSON with its payload validated", () => {
    const e = makeEvent("OrderPlaced", orderId, { customerId: "c", items: [{ sku: "A", quantity: 1 }] });
    expect(parseEvent(JSON.stringify(e))).toEqual(e);
  });

  it("rejects malformed payloads", () => {
    const e = makeEvent("OrderPlaced", orderId, { customerId: "c", items: [{ sku: "A", quantity: 1 }] });
    expect(() => parseEvent(JSON.stringify({ ...e, data: { customerId: "c", items: [] } }))).toThrow();
    expect(() => parseEvent(JSON.stringify({ ...e, type: "Nope" }))).toThrow();
    expect(() => parseEvent("not json")).toThrow();
  });

  it("refuses to create an invalid event", () => {
    expect(() => makeEvent("StockReserved", orderId, { amountCents: -1, customerId: "c" })).toThrow();
  });

  it("each service publishes only to its own topic", () => {
    expect(TOPIC_OF.OrderPlaced).toBe(TOPICS.orders);
    expect(TOPIC_OF.StockReserved).toBe(TOPICS.inventory);
    expect(TOPIC_OF.PaymentFailed).toBe(TOPICS.payments);
  });
});
