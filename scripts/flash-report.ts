/**
 * After a flash sale: wait for every order to settle, then report what matters.
 *
 *   npx tsx scripts/flash-report.ts <initial-stock> <since-iso-timestamp> [mode]
 *
 * - oversold units (must be 0)
 * - stock consistency (available + reserved must equal the starting stock)
 * - how orders ended, and how long each took from placed to CONFIRMED/CANCELLED (p50/p95/p99)
 */
import { writeFile, mkdir } from "node:fs/promises";
import { connect } from "../src/lib/db.ts";

const [stockArg, sinceArg, mode = "locked"] = process.argv.slice(2);
const initial = Number(stockArg ?? 100);
const since = sinceArg ?? new Date(0).toISOString();
const orders = connect("orders", 2);
const inventory = connect("inventory", 2);

// Wait until nothing is still PENDING/RESERVED (or 60 s pass).
const deadline = Date.now() + 60_000;
const openOrders = async () =>
  (
    await orders<{ open: number }[]>`
      select count(*)::int as open from orders o join order_items i on i.order_id = o.id
      where i.sku = 'FLASH-1' and o.created_at >= ${since} and o.status in ('PENDING', 'RESERVED')`
  )[0]!.open;
let open = await openOrders();
while (open > 0 && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 500));
  open = await openOrders();
}

const statuses = await orders<{ status: string; n: number }[]>`
  select o.status, count(*)::int as n from orders o join order_items i on i.order_id = o.id
  where i.sku = 'FLASH-1' and o.created_at >= ${since} group by 1 order by 1`;
const byClient = await orders<{ client: string; status: string; n: number }[]>`
  select case when o.customer_id like 'bot-%' then 'bots' else 'buyers' end as client, o.status, count(*)::int as n
  from orders o join order_items i on i.order_id = o.id
  where i.sku = 'FLASH-1' and o.created_at >= ${since} group by 1, 2 order by 1, 2`;
const [lat] = await orders<{ p50: number; p95: number; p99: number; max: number }[]>`
  select
    percentile_cont(0.5)  within group (order by ms) as p50,
    percentile_cont(0.95) within group (order by ms) as p95,
    percentile_cont(0.99) within group (order by ms) as p99,
    max(ms) as max
  from (
    select extract(epoch from (o.updated_at - o.created_at)) * 1000 as ms
    from orders o join order_items i on i.order_id = o.id
    where i.sku = 'FLASH-1' and o.created_at >= ${since} and o.status in ('CONFIRMED', 'CANCELLED')
  ) t`;
const [stock] = await inventory<{ available: number; reserved: number }[]>`
  select p.available, coalesce(sum(r.quantity) filter (where r.status = 'reserved'), 0)::int as reserved
  from products p left join reservations r on r.sku = p.sku where p.sku = 'FLASH-1' group by p.available`;

const confirmed = statuses.find((s) => s.status === "CONFIRMED")?.n ?? 0;
const report = {
  mode,
  initialStock: initial,
  orders: Object.fromEntries(statuses.map((s) => [s.status, s.n])),
  ordersByClient: byClient.reduce<Record<string, Record<string, number>>>((acc, r) => {
    (acc[r.client] ??= {})[r.status] = r.n;
    return acc;
  }, {}),
  stillOpen: open,
  unitsReserved: stock!.reserved,
  oversold: Math.max(0, stock!.reserved - initial),
  confirmedBeyondStock: Math.max(0, confirmed - initial),
  stockConsistent: stock!.available + stock!.reserved === initial,
  availableAfter: stock!.available,
  settleLatencyMs: Object.fromEntries(Object.entries(lat ?? {}).map(([k, v]) => [k, Math.round(Number(v))])),
};

console.log(JSON.stringify(report, null, 2));
await mkdir("loadtest/results", { recursive: true });
await writeFile(`loadtest/results/report-${mode}.json`, JSON.stringify(report, null, 2) + "\n");
await Promise.all([orders.end(), inventory.end()]);
