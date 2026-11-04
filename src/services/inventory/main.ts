import { config } from "../../lib/config.ts";
import { connect } from "../../lib/db.ts";
import { TOPICS, type Envelope } from "../../lib/events.ts";
import { runService } from "../../lib/service.ts";
import { releaseStock, reserveStock } from "./logic.ts";

await runService({
  name: "inventory",
  port: config.ports.inventory,
  sql: connect("inventory"),
  routes: (app, { sql }) => {
    app.get("/products", async () => sql`select sku, name, price_cents, available from products order by sku`);

    // Internal (not routed by the gateway): restock a SKU for a new sale.
    app.post<{ Body: { sku?: string; stock?: number } }>("/admin/restock", async (req, reply) => {
      const sku = req.body?.sku ?? "FLASH-1";
      const stock = Number(req.body?.stock ?? 100);
      if (!Number.isInteger(stock) || stock < 0 || stock > 10_000) return reply.code(400).send({ error: "Bad stock" });
      // Reservations are never deleted: orders from an earlier sale may still be in flight, and
      // deleting their reservations would break compensation. The stats below count from restocked_at.
      const [row] = await sql`
        update products set available = ${stock}, restocked_at = now(), restocked_with = ${stock}
        where sku = ${sku} returning sku, available, restocked_at`;
      return row ?? reply.code(404).send({ error: "Unknown SKU" });
    });

    // Which reservation strategy is running (shown on the dashboard so a recording can't mislabel it).
    app.get("/mode", async () => ({ reservationMode: config.reservationMode }));

    // Stock plus what is currently reserved — the load test uses this to check for oversells.
    app.get<{ Params: { sku: string } }>("/products/:sku", async (req, reply) => {
      const [row] = await sql`
        select p.sku, p.name, p.price_cents, p.available, p.restocked_at, p.restocked_with,
               -- units reserved in the current sale (since the last restock)
               coalesce(sum(r.quantity) filter (where r.status = 'reserved' and r.created_at >= p.restocked_at), 0)::int as reserved
        from products p left join reservations r on r.sku = p.sku
        where p.sku = ${req.params.sku}
        group by p.sku
      `;
      return row ?? reply.code(404).send({ error: "Unknown SKU" });
    });
  },
  subscriptions: [
    {
      topics: [TOPICS.orders, TOPICS.payments],
      types: ["OrderPlaced", "PaymentFailed"],
      handle: async (_ctx, event, tx) => {
        if (event.type === "OrderPlaced") await reserveStock(tx, event as Envelope<"OrderPlaced">, config.reservationMode);
        else await releaseStock(tx, event.orderId); // compensation
      },
    },
  ],
});
console.log(`inventory reservation mode: ${config.reservationMode}`);
