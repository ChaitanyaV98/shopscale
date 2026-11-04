// k6 flash-sale load test, run through the gateway.
//
//   npm run loadtest            (see scripts/loadtest.sh: resets stock, runs this, prints the report)
//
// Scenario "buyers": BUYERS distinct customers each try once to buy 1 unit of FLASH-1.
// Scenario "bots":   a few clients hammer the API to show the per-customer rate limit holding.
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://host.docker.internal:4000";
const BUYERS = Number(__ENV.BUYERS || 2000);
const RUN = __ENV.RUN_ID || `${Date.now()}`;

const accepted = new Counter("orders_accepted");
const rateLimited = new Counter("bot_requests_rate_limited");
const botAllowed = new Counter("bot_requests_allowed");

export const options = {
  scenarios: {
    buyers: {
      executor: "shared-iterations",
      vus: Number(__ENV.VUS || 200),
      iterations: BUYERS,
      maxDuration: "2m",
      exec: "buyer",
    },
    bots: {
      executor: "constant-arrival-rate",
      rate: 100,
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 20,
      exec: "bot",
    },
  },
  thresholds: {
    // The gateway + orders service must accept the rush quickly.
    "http_req_duration{scenario:buyers}": ["p(95)<1000"], // regression guard; measured ~500–660 ms on an 8 GB laptop running everything
    "checks{scenario:buyers}": ["rate>0.99"],
  },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

function order(customer, key) {
  return http.post(
    `${BASE}/orders`,
    JSON.stringify({ customerId: customer, items: [{ sku: "FLASH-1", quantity: 1 }] }),
    {
      headers: { "Content-Type": "application/json", "Idempotency-Key": key, "X-Customer-Id": customer },
      tags: { name: "POST /orders" },
    },
  );
}

export function buyer() {
  const id = `${RUN}-${__VU}-${__ITER}`;
  const res = order(`buyer-${id}`, `flash-${id}`);
  if (check(res, { "order accepted (202)": (r) => r.status === 202 })) accepted.add(1);
}

export function bot() {
  const botId = `bot-${RUN}-${__VU % 5}`;
  const res = order(botId, `bot-${RUN}-${__VU}-${__ITER}-${Math.random()}`);
  if (res.status === 429) rateLimited.add(1);
  else botAllowed.add(1);
}
