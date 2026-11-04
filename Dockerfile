# One image for every service; SERVICE picks which one runs.
# No build step: Node 24 runs the TypeScript sources directly by stripping the types.
FROM node:24-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
# Production dependencies only (no tsx/typescript/vitest). The Kafka client ships prebuilt
# librdkafka binaries for linux glibc, so slim (not alpine/musl) is the right base.
RUN npm ci --omit=dev && npm cache clean --force

FROM node:24-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src
COPY scripts/migrate.ts scripts/seed.ts ./scripts/
COPY migrations ./migrations
USER node
# SERVICE=orders|inventory|payments|gateway, or override the command (e.g. node scripts/migrate.ts).
ENV SERVICE=gateway
CMD ["sh", "-c", "exec node src/services/$SERVICE/main.ts"]
