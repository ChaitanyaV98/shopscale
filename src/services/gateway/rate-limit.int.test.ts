import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import { config } from "../../lib/config.ts";
import { createRateLimiter } from "./rate-limit.ts";

const redis = new Redis(config.redisUrl);
const limit = createRateLimiter(redis);
afterAll(() => redis.disconnect());
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("token bucket (real Redis)", () => {
  it("allows a burst up to capacity, then refuses with a retry hint", async () => {
    const key = randomUUID();
    const results = [];
    for (let i = 0; i < 7; i++) results.push(await limit(key, { capacity: 5, refillPerSec: 1 }));
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, true, true, false, false]);
    expect(results[5]!.retryAfterMs).toBeGreaterThan(0);
    expect(results[5]!.retryAfterMs).toBeLessThanOrEqual(1000);
  });

  it("refills over time", async () => {
    const key = randomUUID();
    const bucket = { capacity: 2, refillPerSec: 20 }; // one token every 50 ms
    await limit(key, bucket);
    await limit(key, bucket);
    expect((await limit(key, bucket)).allowed).toBe(false);
    await sleep(120);
    expect((await limit(key, bucket)).allowed).toBe(true);
  });

  it("is atomic: 100 concurrent requests on a 10-token bucket admit exactly 10", async () => {
    const key = randomUUID();
    const results = await Promise.all(Array.from({ length: 100 }, () => limit(key, { capacity: 10, refillPerSec: 0.001 })));
    expect(results.filter((r) => r.allowed)).toHaveLength(10);
  });

  it("keeps clients separate", async () => {
    const a = randomUUID();
    const b = randomUUID();
    await limit(a, { capacity: 1, refillPerSec: 0.001 });
    expect((await limit(a, { capacity: 1, refillPerSec: 0.001 })).allowed).toBe(false);
    expect((await limit(b, { capacity: 1, refillPerSec: 0.001 })).allowed).toBe(true);
  });
});
