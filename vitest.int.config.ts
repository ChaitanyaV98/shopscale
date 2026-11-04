import { defineConfig } from "vitest/config";

// Needs the docker-compose stack (npm run infra:up && npm run migrate).
export default defineConfig({
  test: { include: ["src/**/*.int.test.ts"], fileParallelism: false, testTimeout: 60_000, hookTimeout: 60_000 },
});
