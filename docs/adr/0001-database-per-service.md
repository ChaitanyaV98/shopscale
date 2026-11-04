# 1. Each service owns its own database

**Status:** accepted

## Context
Orders, inventory and payments change for different reasons and at different rates. If they share
tables, any service can bypass another's rules (e.g. decrement stock without a reservation), and a
schema change in one breaks the others.

## Decision
Each service has its own Postgres database and user (`orders`, `inventory`, `payments`). No service
can connect to another's database (`REVOKE CONNECT … FROM PUBLIC`). Services only learn about each
other's changes through events.

Locally all three databases live on one Postgres server to keep the laptop setup light; they are
still fully separate databases with separate credentials, so moving one to its own server is a
connection-string change.

## Consequences
- No cross-service joins or transactions. Consistency across services is eventual, handled by
  events (ADR 2) and a saga (ADR 3).
- Each service can be scaled, migrated and deployed on its own.
- Reporting across services needs events or a read model, not a SQL join.

## Alternatives considered
- **Shared database, separate schemas:** simpler, but nothing stops a service reaching into another's tables.
- **One database per service on separate servers locally:** truer to production, but too heavy for an 8 GB laptop.
