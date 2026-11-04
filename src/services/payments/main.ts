import { z } from "zod";
import { config } from "../../lib/config.ts";
import { connect } from "../../lib/db.ts";
import { TOPICS, type Envelope } from "../../lib/events.ts";
import { runService } from "../../lib/service.ts";
import { chargeForOrder, recordPayment, type Charge } from "./logic.ts";

const sql = connect("payments");

await runService({
  name: "payments",
  port: config.ports.payments,
  sql,
  routes: (app) => {
    app.get<{ Params: { orderId: string } }>("/payments/:orderId", async (req, reply) => {
      if (!z.string().uuid().safeParse(req.params.orderId).success) return reply.code(400).send({ error: "Bad order id" });
      const [row] = await sql`select * from payments where order_id = ${req.params.orderId}`;
      return row ?? reply.code(404).send({ error: "No payment for this order" });
    });
  },
  subscriptions: [
    {
      topics: [TOPICS.inventory],
      types: ["StockReserved"],
      prepare: (_ctx, event) => chargeForOrder(sql, event as Envelope<"StockReserved">),
      handle: (_ctx, event, tx, charge) => recordPayment(tx, event as Envelope<"StockReserved">, charge as Charge),
    },
  ],
});
