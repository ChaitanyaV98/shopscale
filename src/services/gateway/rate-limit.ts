import type { Redis } from "ioredis";

/**
 * Token bucket in Redis.
 *
 * Each client has a bucket of `capacity` tokens that refills at `refillPerSec`. A request takes one
 * token; an empty bucket means 429. Bursts up to `capacity` are allowed, the sustained rate is
 * `refillPerSec`.
 *
 * Why a Lua script: read-refill-take-write must be one atomic step, or two gateway replicas could
 * both see "1 token left" and both let a request through. Redis runs a script without interleaving.
 * Why Redis TIME (not the gateway's clock): replicas with drifting clocks would refill differently.
 */
const SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_ms = tonumber(ARGV[2]) / 1000
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1]) or capacity
local ts = tonumber(state[2]) or now

tokens = math.min(capacity, tokens + (now - ts) * refill_per_ms)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = math.ceil((cost - tokens) / refill_per_ms)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- A full bucket carries no information, so let idle keys expire instead of piling up.
redis.call('PEXPIRE', key, math.ceil(capacity / refill_per_ms) + 1000)
return { allowed, math.floor(tokens), retry_after }
`;

export interface BucketConfig {
  capacity: number;
  refillPerSec: number;
}

export interface Decision {
  allowed: boolean;
  remaining: number;
  retryAfterMs: number;
}

export function createRateLimiter(redis: Redis) {
  redis.defineCommand("takeToken", { numberOfKeys: 1, lua: SCRIPT });
  const take = (redis as unknown as { takeToken: (...a: (string | number)[]) => Promise<[number, number, number]> }).takeToken.bind(redis);

  return async (key: string, bucket: BucketConfig, cost = 1): Promise<Decision> => {
    const [allowed, remaining, retryAfterMs] = await take(`ratelimit:${key}`, bucket.capacity, bucket.refillPerSec, cost);
    return { allowed: allowed === 1, remaining, retryAfterMs };
  };
}
