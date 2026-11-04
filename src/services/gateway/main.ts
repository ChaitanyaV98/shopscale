import { readFileSync } from "node:fs";
import Fastify from "fastify";
import { Redis } from "ioredis";
import { config } from "../../lib/config.ts";
import { createRateLimiter, type BucketConfig } from "./rate-limit.ts";

/**
 * The single public entry point: rate limiting, then routing to the service that owns the data.
 * The services themselves are not exposed (in Kubernetes they're ClusterIP-only).
 */
const upstream = {
  orders: process.env.ORDERS_URL ?? `http://localhost:${config.ports.orders}`,
  inventory: process.env.INVENTORY_URL ?? `http://localhost:${config.ports.inventory}`,
  payments: process.env.PAYMENTS_URL ?? `http://localhost:${config.ports.payments}`,
};

// Writes are expensive and abuse-prone; reads are cheap. Both configurable for load tests.
const WRITE: BucketConfig = { capacity: Number(process.env.RL_WRITE_BURST ?? 10), refillPerSec: Number(process.env.RL_WRITE_PER_SEC ?? 5) };
const READ: BucketConfig = { capacity: Number(process.env.RL_READ_BURST ?? 100), refillPerSec: Number(process.env.RL_READ_PER_SEC ?? 50) };

const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 1, enableOfflineQueue: false });
redis.on("error", (e) => console.warn("[gateway] redis:", e.message));
const limit = createRateLimiter(redis);

const app = Fastify({ logger: { level: process.env.LOG_LEVEL ?? "warn" }, trustProxy: true });

app.get("/health", async () => ({ ok: true, service: "gateway", redis: redis.status }));

// Flash-sale dashboard: a read-only view of one SKU's sale (used for the demo recording).
const dashboardHtml = readFileSync(new URL("./dashboard.html", import.meta.url), "utf8");
app.get("/dashboard", async (_req, reply) => reply.type("text/html").send(dashboardHtml));
app.get<{ Querystring: { since?: string; sku?: string } }>("/dashboard/data", async (req) => {
  const sku = encodeURIComponent(req.query.sku ?? "FLASH-1");
  const since = encodeURIComponent(req.query.since ?? new Date(0).toISOString());
  const get = async (url: string) => (await fetch(url, { signal: AbortSignal.timeout(3000) })).json();
  const [orders, product, mode] = await Promise.all([
    get(`${upstream.orders}/stats?sku=${sku}&since=${since}`),
    get(`${upstream.inventory}/products/${sku}`),
    get(`${upstream.inventory}/mode`),
  ]);
  return { orders, available: product.available, reserved: product.reserved, mode: mode.reservationMode };
});

/**
 * Who is this client? In production: the user id from a verified auth token. This demo has no auth,
 * so it uses an X-Customer-Id header (simulating that token), falling back to the client IP.
 */
const clientKey = (headers: Record<string, unknown>, ip: string) => {
  const id = headers["x-customer-id"];
  return typeof id === "string" && /^[\w-]{1,64}$/.test(id) ? `customer:${id}` : `ip:${ip}`;
};

app.addHook("onRequest", async (req, reply) => {
  if (req.url === "/health" || req.url.startsWith("/dashboard")) return; // monitoring isn't customer traffic
  const bucket = req.method === "GET" ? READ : WRITE;
  const kind = req.method === "GET" ? "read" : "write";
  try {
    const d = await limit(`${kind}:${clientKey(req.headers, req.ip)}`, bucket);
    reply.header("RateLimit-Limit", bucket.capacity).header("RateLimit-Remaining", d.remaining);
    if (!d.allowed) {
      const retryAfter = Math.max(1, Math.ceil(d.retryAfterMs / 1000));
      return reply.code(429).header("Retry-After", retryAfter).send({ error: "Too many requests", retryAfterSeconds: retryAfter });
    }
  } catch (e) {
    // Fail OPEN: the limiter protects capacity, not correctness (row locks prevent overselling
    // with or without it), so a Redis outage shouldn't take the whole shop down.
    req.log.warn({ err: (e as Error).message }, "rate limiter unavailable, allowing request");
  }
});

/** Forward a request to a service and relay its status, selected headers and body. */
async function forward(target: string, path: string, req: { method: string; headers: Record<string, unknown>; body?: unknown }) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  for (const h of ["idempotency-key", "x-request-id"]) if (typeof req.headers[h] === "string") headers[h] = req.headers[h] as string;
  const res = await fetch(`${target}${path}`, {
    method: req.method,
    headers,
    body: req.method === "GET" ? undefined : JSON.stringify(req.body ?? {}),
    signal: AbortSignal.timeout(5000),
  });
  return { status: res.status, replayed: res.headers.get("idempotent-replayed"), body: await res.json().catch(() => ({})) };
}

const routes: { method: "GET" | "POST"; url: string; to: keyof typeof upstream }[] = [
  { method: "POST", url: "/orders", to: "orders" },
  { method: "GET", url: "/orders/:id", to: "orders" },
  { method: "GET", url: "/products", to: "inventory" },
  { method: "GET", url: "/products/:sku", to: "inventory" },
  { method: "GET", url: "/payments/:orderId", to: "payments" },
];

for (const r of routes) {
  app.route({
    method: r.method,
    url: r.url,
    handler: async (req, reply) => {
      try {
        const res = await forward(upstream[r.to], req.url, req);
        if (res.replayed) reply.header("Idempotent-Replayed", res.replayed);
        return reply.code(res.status).send(res.body);
      } catch (e) {
        req.log.error({ err: (e as Error).message, to: r.to }, "upstream failed");
        return reply.code(502).send({ error: `${r.to} service unavailable` });
      }
    },
  });
}

await app.listen({ port: config.ports.gateway, host: "0.0.0.0" });
console.log(`gateway listening on :${config.ports.gateway} (write ${WRITE.refillPerSec}/s burst ${WRITE.capacity}, read ${READ.refillPerSec}/s burst ${READ.capacity})`);

const shutdown = async () => {
  await app.close();
  redis.disconnect();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
