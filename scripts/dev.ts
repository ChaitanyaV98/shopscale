/**
 * Start every service in one terminal, with prefixed logs: `npm run dev`. Ctrl+C stops them all.
 * Extra env (e.g. RESERVATION_MODE=naive) is passed through.
 */
import { spawn, type ChildProcess } from "node:child_process";

const SERVICES = (process.env.SERVICES ?? "orders,inventory,payments,gateway").split(",");
const procs: ChildProcess[] = [];

for (const name of SERVICES) {
  const p = spawn("npx", ["tsx", `src/services/${name}/main.ts`], { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
  const tag = `[${name}]`.padEnd(12);
  const pipe = (stream: NodeJS.ReadableStream, out: NodeJS.WriteStream) =>
    stream.on("data", (c: Buffer) => c.toString().split("\n").filter(Boolean).forEach((l) => out.write(`${tag}${l}\n`)));
  pipe(p.stdout!, process.stdout);
  pipe(p.stderr!, process.stderr);
  p.on("exit", (code) => console.log(`${tag}exited (${code})`));
  procs.push(p);
}

const stop = () => procs.forEach((p) => p.kill("SIGTERM"));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
