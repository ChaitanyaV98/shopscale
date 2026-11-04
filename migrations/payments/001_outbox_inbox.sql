-- Outbox: events written in the same transaction as the business change (see src/lib/outbox.ts).
create table outbox (
  seq bigserial primary key,            -- publish order
  id uuid not null unique,              -- the event id consumers de-duplicate on
  topic text not null,
  key text not null,                    -- Kafka key = order id, so one order's events stay ordered
  payload jsonb not null,
  created_at timestamptz not null default now(),
  published_at timestamptz
);
-- The relay only ever looks at unpublished rows: a partial index keeps that scan tiny as the table grows.
create index outbox_unpublished_idx on outbox (seq) where published_at is null;

-- Inbox: ids of events already handled, so a redelivered event has no second effect (src/lib/inbox.ts).
create table inbox (
  event_id uuid primary key,
  event_type text not null,
  processed_at timestamptz not null default now()
);
