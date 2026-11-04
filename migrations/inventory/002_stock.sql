create table products (
  sku text primary key,
  name text not null,
  price_cents integer not null check (price_cents >= 0),
  -- Safety net only: it stops a negative count, but NOT a lost update (two buyers both reading 1
  -- and both writing 0). Preventing that is the job of the row lock (SELECT … FOR UPDATE).
  available integer not null check (available >= 0)
);

create table reservations (
  order_id uuid not null,
  sku text not null references products (sku),
  quantity integer not null check (quantity > 0),
  status text not null check (status in ('reserved', 'released')),
  created_at timestamptz not null default now(),
  primary key (order_id, sku)
);
-- Composite index for "how many units of this SKU are currently reserved" — the oversell check.
create index reservations_sku_status_idx on reservations (sku, status);
