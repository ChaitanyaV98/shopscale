-- One payment per order, enforced by the primary key: the second attempt for the same order
-- can't create a second charge.
create table payments (
  order_id uuid primary key,
  payment_id uuid not null,
  amount_cents integer not null,
  status text not null check (status in ('captured', 'failed')),
  failure_reason text,
  created_at timestamptz not null default now()
);

-- Stand-in for the payment provider's side (e.g. Stripe), which de-duplicates on the
-- Idempotency-Key we send. Kept in the database so it survives restarts like the real thing.
create table provider_charges (
  idempotency_key text primary key,
  charge_id uuid not null,
  amount_cents integer not null,
  succeeded boolean not null,
  created_at timestamptz not null default now()
);
