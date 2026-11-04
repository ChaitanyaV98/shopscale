import { z } from "zod";
import { config } from "../../lib/config.ts";
import { connect } from "../../lib/db.ts";
import { TOPICS } from "../../lib/events.ts";
import { runService } from "../../lib/service.ts";
import { applyEvent, getOrder, placeOrder } from "./logic.ts";

const Body = z.object({
  customerId: z.string().min(1).max(100),
  items: z
    .array(z.object({ sku: z.string().min(1).max(64), quantity: z.number().int().min(1).max(100) }))
    .min(1)
    .max(50)
    .refine((items) => new Set(items.map((i) => i.sku)).size === items.length, "Each SKU may appear only once"),
});

await runService({
  name: "orders",
  port: config.ports.orders,
  sql: connect("orders"),
  routes: (app, { sql, relay }) => {
    // Accepts the order and returns at once (202): reservation and payment happen asynchronously.
    app.post("/orders", async (req, reply) => {
      const key = req.headers["idempotency-key"];
      if (typeof key !== "string" || key.length < 8 || key.length > 200) {
        return reply.code(400).send({ error: "Send an Idempotency-Key header (8–200 chars), e.g. a UUID per purchase attempt." });
      }
      const body = Body.safeParse(req.body);
      if (!body.success) return reply.code(400).send({ error: body.error.issues[0]?.message ?? "Invalid body" });

      const result = await placeOrder(sql, { idempotencyKey: key, ...body.data });
      if (result.kind === "key_reused") {
        return reply.code(422).send({ error: "This Idempotency-Key was already used for a different request.", orderId: result.orderId });
      }
      if (result.kind === "created") relay.wake();
      return reply
        .code(result.kind === "created" ? 202 : 200)
        .header("Idempotent-Replayed", String(result.kind === "replayed"))
        .send({ orderId: result.orderId, status: result.status });
    });

    app.get<{ Params: { id: string } }>("/orders/:id", async (req, reply) => {
      if (!z.string().uuid().safeParse(req.params.id).success) return reply.code(400).send({ error: "Bad order id" });
      const order = await getOrder(sql, req.params.id);
      return order ? order : reply.code(404).send({ error: "Order not found" });
    });
  },
  subscriptions: [
    {
      topics: [TOPICS.inventory, TOPICS.payments],
      types: ["StockReserved", "StockRejected", "PaymentCaptured", "PaymentFailed"],
      handle: (_ctx, event, tx) => applyEvent(tx, event),
    },
  ],
});
