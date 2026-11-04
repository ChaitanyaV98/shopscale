/**
 * Seed the catalogue, or reset one SKU's stock before a load test.
 *
 *   npm run seed                        # catalogue with a 100-unit flash-sale item
 *   npm run seed -- FLASH-1 100         # reset FLASH-1 to 100 units and clear its reservations
 */
import { connect } from "../src/lib/db.ts";

const PRODUCTS = [
  { sku: "FLASH-1", name: "Flash-sale headphones", price_cents: 199_900, available: 100 },
  { sku: "TSHIRT-M", name: "T-shirt (M)", price_cents: 49_900, available: 1_000 },
  { sku: "MUG", name: "Coffee mug", price_cents: 29_900, available: 1_000 },
  { sku: "LAPTOP-13", name: "13-inch laptop", price_cents: 8_999_900, available: 50 },
];

const sql = connect("inventory", 1);
const [sku, stock] = process.argv.slice(2);

if (sku) {
  await sql.begin(async (tx) => {
    await tx`delete from reservations where sku = ${sku}`;
    const r = await tx`update products set available = ${Number(stock ?? 100)} where sku = ${sku}`;
    if (r.count === 0) throw new Error(`Unknown SKU ${sku}`);
  });
  console.log(`${sku} reset to ${stock ?? 100} units`);
} else {
  for (const p of PRODUCTS) {
    await sql`
      insert into products ${sql(p)}
      on conflict (sku) do update set name = excluded.name, price_cents = excluded.price_cents, available = excluded.available
    `;
  }
  console.log(`seeded ${PRODUCTS.length} products`);
}
await sql.end();
