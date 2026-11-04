/**
 * All-in-one mode (SERVICE=all) for single-container hosts such as Render's free tier: run the
 * migrations, then start every service as its own child process. The gateway listens on $PORT;
 * the other services stay on localhost. If any service dies, everything exits so the host restarts
 * the container cleanly. Elsewhere (Compose, Kubernetes) each service runs in its own container.
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";

const node = process.execPath;
const step = (script: string) => {
  const r = spawnSync(node, [script], { stdio: "inherit", env: process.env });
  if (r.status !== 0) process.exit(r.status ?? 1);
};

// Off in production, where the app's database role can't change the schema (migrations run
// separately with the owner role).
if (process.env.MIGRATE_ON_START !== "false") step("scripts/migrate.ts");
if (process.env.SEED_ON_START !== "false") step("scripts/seed.ts");

const env = { ...process.env, GATEWAY_PORT: process.env.PORT ?? process.env.GATEWAY_PORT ?? "4000" };
const children: ChildProcess[] = ["orders", "inventory", "payments", "gateway"].map((name) => {
  const child = spawn(node, [`src/services/${name}/main.ts`], { stdio: "inherit", env: { ...env, SERVICE: name } });
  child.on("exit", (code) => {
    if (stopping) return;
    console.error(`[all] ${name} exited (${code}); stopping everything so the host restarts us`);
    shutdown(1);
  });
  return child;
});

let stopping = false;
function shutdown(code = 0) {
  if (stopping) return;
  stopping = true;
  children.forEach((c) => c.kill("SIGTERM")); // each service drains its outbox and exits
  setTimeout(() => process.exit(code), 10_000).unref();
  Promise.all(children.map((c) => new Promise((r) => (c.exitCode !== null ? r(null) : c.on("exit", r))))).then(() => process.exit(code));
}
process.on("SIGTERM", () => shutdown(0));
process.on("SIGINT", () => shutdown(0));
