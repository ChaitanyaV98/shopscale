import { randomUUID } from "node:crypto";
import { z } from "zod";

/**
 * The contract between services. Every event has a unique id (used for de-duplication), a type,
 * the order it belongs to (used as the Kafka key, so all events for one order stay in order on one
 * partition), and a typed payload.
 */
export const TOPICS = {
  orders: "orders.events",
  inventory: "inventory.events",
  payments: "payments.events",
} as const;

const Item = z.object({ sku: z.string(), quantity: z.number().int().positive() });

export const EventSchemas = {
  OrderPlaced: z.object({ customerId: z.string(), items: z.array(Item).min(1) }),
  StockReserved: z.object({ amountCents: z.number().int().nonnegative(), customerId: z.string() }),
  StockRejected: z.object({ reason: z.string() }),
  StockReleased: z.object({}),
  PaymentCaptured: z.object({ paymentId: z.string(), amountCents: z.number().int() }),
  PaymentFailed: z.object({ reason: z.string() }),
} as const;

export type EventType = keyof typeof EventSchemas;
export type EventData<T extends EventType> = z.infer<(typeof EventSchemas)[T]>;

export interface Envelope<T extends EventType = EventType> {
  id: string;
  type: T;
  orderId: string;
  occurredAt: string;
  data: EventData<T>;
}

/** Which topic each event type is published to (one topic per producing service). */
export const TOPIC_OF: Record<EventType, string> = {
  OrderPlaced: TOPICS.orders,
  StockReserved: TOPICS.inventory,
  StockRejected: TOPICS.inventory,
  StockReleased: TOPICS.inventory,
  PaymentCaptured: TOPICS.payments,
  PaymentFailed: TOPICS.payments,
};

export function makeEvent<T extends EventType>(type: T, orderId: string, data: EventData<T>): Envelope<T> {
  return { id: randomUUID(), type, orderId, occurredAt: new Date().toISOString(), data: EventSchemas[type].parse(data) as EventData<T> };
}

const EnvelopeShape = z.object({
  id: z.string().uuid(),
  type: z.enum(Object.keys(EventSchemas) as [EventType, ...EventType[]]),
  orderId: z.string().uuid(),
  occurredAt: z.string(),
  data: z.unknown(),
});

/**
 * Validate a message from Kafka. Throws on anything malformed; the consumer (kafka.ts) then logs it
 * with its topic/partition/offset and skips it. There is no dead-letter topic yet.
 */
export function parseEvent(raw: string): Envelope {
  const env = EnvelopeShape.parse(JSON.parse(raw));
  return { ...env, data: EventSchemas[env.type].parse(env.data) } as Envelope;
}
