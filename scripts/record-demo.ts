/**
 * Record the README/portfolio demo: open the flash-sale dashboard, run the k6 flash sale, and save
 * a screenshot every 400 ms until every order has settled. Services must already be running in the
 * mode you want to show (RESERVATION_MODE=naive or locked).
 *
 *   npx tsx scripts/record-demo.ts <out-dir>
 */
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import path from "node:path";
import { chromium } from "@playwright/test";

const out = process.argv[2];
if (!out) throw new Error("usage: record-demo.ts <out-dir>");
mkdirSync(out, { recursive: true });

spawnSync("npx", ["tsx", "scripts/seed.ts", "FLASH-1", "100"], { stdio: "inherit" });
const since = new Date().toISOString();

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1000, height: 640 }, colorScheme: "dark" });
await page.goto(`http://localhost:4000/dashboard?stock=100&since=${encodeURIComponent(since)}`);
await page.waitForTimeout(800);

let n = 0;
const shot = () => page.screenshot({ path: path.join(out, `${String(n++).padStart(4, "0")}.png`) });
for (let i = 0; i < 3; i++) await shot(); // idle frames before the rush

const k6 = spawn(
  "docker",
  ["run", "--rm", "-v", `${process.cwd()}/loadtest:/scripts`, "-e", "BASE_URL=http://host.docker.internal:4000",
   "-e", "BUYERS=2000", "-e", "VUS=200", "-e", `RUN_ID=demo-${Date.now()}`, "grafana/k6:0.54.0", "run", "--quiet", "/scripts/flash-sale.js"],
  { stdio: "ignore" },
);
let k6Done = false;
k6.on("exit", () => (k6Done = true));

const deadline = Date.now() + 120_000;
while (Date.now() < deadline) {
  await shot();
  const verdict = (await page.locator("#verdict").textContent()) ?? "";
  if (k6Done && !verdict.startsWith("Waiting") && !verdict.startsWith("Processing")) break;
  await page.waitForTimeout(400);
}
for (let i = 0; i < 2; i++) await shot();
console.log(`${n} frames → ${out}: ${await page.locator("#verdict").textContent()}`);
await browser.close();
