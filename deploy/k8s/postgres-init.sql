-- Database per service: each service owns its data and can only reach the others through events.
-- (One Postgres server keeps the laptop setup light; the databases and users are fully separate.)
CREATE USER orders_svc PASSWORD 'orders_svc';
CREATE USER inventory_svc PASSWORD 'inventory_svc';
CREATE USER payments_svc PASSWORD 'payments_svc';
CREATE DATABASE orders OWNER orders_svc;
CREATE DATABASE inventory OWNER inventory_svc;
CREATE DATABASE payments OWNER payments_svc;
REVOKE CONNECT ON DATABASE orders, inventory, payments FROM PUBLIC;
