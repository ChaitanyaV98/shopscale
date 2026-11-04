/**
 * All configuration comes from the environment, with defaults that match docker-compose.yml.
 * Each service only reads the settings it needs (its own database, Kafka, its port).
 */
const env = (key: string, fallback: string) => process.env[key] ?? fallback;

export const config = {
  kafkaBrokers: env("KAFKA_BROKERS", "localhost:19092").split(","),
  redisUrl: env("REDIS_URL", "redis://localhost:6381"),
  databases: {
    orders: env("ORDERS_DATABASE_URL", "postgres://orders_svc:orders_svc@localhost:5436/orders"),
    inventory: env("INVENTORY_DATABASE_URL", "postgres://inventory_svc:inventory_svc@localhost:5436/inventory"),
    payments: env("PAYMENTS_DATABASE_URL", "postgres://payments_svc:payments_svc@localhost:5436/payments"),
  },
  ports: {
    gateway: Number(env("GATEWAY_PORT", "4000")),
    orders: Number(env("ORDERS_PORT", "4001")),
    inventory: Number(env("INVENTORY_PORT", "4002")),
    payments: Number(env("PAYMENTS_PORT", "4003")),
  },
  /**
   * "locked" (default): reserve stock with SELECT … FOR UPDATE.
   * "naive": read, then write, with no lock. Exists only so the load test can show the oversell it prevents.
   */
  reservationMode: env("RESERVATION_MODE", "locked") as "locked" | "naive",
};

export type ServiceName = keyof typeof config.databases;
