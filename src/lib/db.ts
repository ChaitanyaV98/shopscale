import postgres from "postgres";
import { config, type ServiceName } from "./config.ts";

export type Sql = postgres.Sql;
export type Tx = postgres.TransactionSql;

/** A connection pool to one service's own database. */
export function connect(service: ServiceName, max = 20): Sql {
  return postgres(config.databases[service], {
    max,
    onnotice: () => {},
    // bigint/numeric come back as strings by default; counts and cents fit safely in a JS number.
    types: { bigint: postgres.BigInt },
    transform: { undefined: null },
  });
}
