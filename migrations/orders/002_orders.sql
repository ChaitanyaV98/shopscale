create table orders (
  id uuid primary key,
  customer_id text not null,
  -- PENDING → RESERVED → CONFIRMED, or → CANCELLED (out of stock / payment failed)
  status text not null check (status in ('PENDING', 'RESERVED', 'CONFIRMED', 'CANCELLED')),
  cancel_reason text,
  total_cents integer,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
-- Composite index for "my recent orders": equality on customer_id, then sorted by time.
create index orders_customer_created_idx on orders (customer_id, created_at desc);

create table order_items (
  order_id uuid not null references orders (id) on delete cascade,
  sku text not null,
  quantity integer not null check (quantity > 0),
  primary key (order_id, sku)
);

-- Idempotency keys: a retried POST /orders with the same key returns the original order
-- instead of creating a second one. request_hash catches a key reused for a different request.
create table idempotency_keys (
  key text primary key,
  request_hash text not null,
  order_id uuid not null references orders (id),
  created_at timestamptz not null default now()
);
