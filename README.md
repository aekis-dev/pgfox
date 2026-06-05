# <img src="docs/logo.jpg" width="256" height="256" />
# PgFox

PgFox is a transparent PostgreSQL connection pooler written in Go. It sits
between your application and PostgreSQL and lets many client connections share a
small number of backend connections, without the application noticing a proxy is
present.

Unlike pooler configurations that force you to give up features in exchange for
efficiency, PgFox keeps full PostgreSQL semantics — transactions, prepared
statements, LISTEN/NOTIFY, and query cancellation all work — while still
multiplexing clients onto a small backend pool.

```
┌─────────────┐        ┌─────────────┐        ┌─────────────┐
│   Client    │◄──────►│   PgFox     │◄──────►│ PostgreSQL  │
│ (Odoo, etc.)│  SCRAM │   pooler    │  TLS   │   backend   │
└─────────────┘        └─────────────┘  cert  └─────────────┘
```

## What makes it transparent

- **Client authentication is real.** PgFox speaks SCRAM-SHA-256 to the client
  and verifies the password against the role's stored verifier in PostgreSQL —
  it is not a pass-the-password shim and stores no credentials in its config.
- **Backend authentication uses certificates.** PgFox connects to PostgreSQL
  with a per-user TLS client certificate it generates and signs itself, so no
  database passwords live in the pooler.
- **Prepared statements are shared safely.** PgFox rewrites prepared statements
  to internal, content-addressed names and deploys each one to a backend only
  once, so clients reusing the same query share the same parsed statement across
  pooled connections.
- **Query cancellation works end to end.** Each client gets its own cancel key;
  a cancel request is routed to whichever backend is currently running that
  client's query.

## Quick start

```bash
# Build
go build -o pgfox ./...

# Run with a config file
./pgfox --config config.yaml
```

Point a client at PgFox's listen address (`:5433` in the sample config):

```bash
psql "host=localhost port=5433 dbname=mydb user=myrole sslmode=disable"
```

PgFox authenticates the client with SCRAM, opens (or reuses) a backend
connection to the target PostgreSQL using a certificate for `myrole`, and routes
queries through the pool.

## Documentation

- [Architecture](docs/architecture.md) — how pooling, prepared-statement
  multiplexing, transactions, LISTEN/NOTIFY, and cancellation actually work.
- [Configuration](docs/configuration.md) — every config field, with examples
  drawn from the sample `config.yaml`.
- [Startup & wire protocol](docs/protocol.md) — the connection, authentication,
  and cancellation flows at the protocol level.
- [Usage scenarios](docs/usage.md) — what happens for regular queries,
  transactions, prepared statements (asyncpg / psycopg2), LISTEN/NOTIFY, and
  cancellation.
- [Playbook](docs/playbook.md) - a guide to understand how PgFox works under the hood.

## License

AGPL v3 — see the LICENSE file.
