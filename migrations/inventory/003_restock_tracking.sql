-- When a SKU was last restocked and with how many units, so the dashboard can show the latest
-- sale (orders since the restock) and compute oversold units against the right starting stock.
alter table products
  add column restocked_at timestamptz not null default now(),
  add column restocked_with integer;
update products set restocked_with = available;
