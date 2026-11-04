/**
 * Apply each service's SQL migrations to that service's own database, in file order.
 * Applied files are recorded in schema_migrations, so running this again only applies new ones.
 *
 *   npm run migrate
 *
 * If <SERVICE>_OWNER_URL is set (production), migrations run as that owner role instead of the
 * app's DML-only role.
 */
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { connect } from "../src/lib/db.ts";
import { config, type ServiceName } from "../src/lib/config.ts";

const SERVICES: ServiceName[] = ["orders", "inventory", "payments"];

async function migrate(service: ServiceName) {
  // In production the app connects as a DML-only role; schema changes need the owner role.
  // (config is read once at import, so override it directly rather than via process.env)
  const ownerUrl = process.env[`${service.toUpperCase()}_OWNER_URL`];
  if (ownerUrl) config.databases[service] = ownerUrl;
  const sql = connect(service, 1);
  try {
    await sql`create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())`;
    const done = new Set((await sql<{ name: string }[]>`select name from schema_migrations`).map((r) => r.name));
    const dir = path.join("migrations", service);
    for (const file of (await readdir(dir)).filter((f) => f.endsWith(".sql")).sort()) {
      if (done.has(file)) continue;
      const body = await readFile(path.join(dir, file), "utf8");
      await sql.begin(async (tx) => {
        await tx.unsafe(body);
        await tx`insert into schema_migrations (name) values (${file})`;
      });
      console.log(`${service}: applied ${file}`);
    }
  } finally {
    await sql.end();
  }
}

for (const s of SERVICES) await migrate(s);
console.log("migrations up to date");
