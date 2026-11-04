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

    // Which reservation strategy is running (shown on the dashboard so a recording can't mislabel it).
    app.get("/mode", async () => ({ reservationMode: config.reservationMode }));

    // Stock plus what is currently reserved — the load test uses this to check for oversells.
    app.get<{ Params: { sku: string } }>("/products/:sku", async (req, reply) => {
      const [row] = await sql`
        select p.sku, p.name, p.price_cents, p.available,
               coalesce(sum(r.quantity) filter (where r.status = 'reserved'), 0)::int as reserved
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
