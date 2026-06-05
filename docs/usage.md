# Usage scenarios

This document walks through what PgFox does for the kinds of workloads a client
throws at it. None of these require application changes — PgFox is transparent,
so clients connect and behave as if talking directly to PostgreSQL.

All examples assume the sample configuration (PgFox listening on `:5433`,
forwarding to a PostgreSQL backend on `127.0.0.1:5432`).

## Connecting

```bash
psql "host=localhost port=5433 dbname=mydb user=myrole sslmode=disable"
```

(The port is whatever you set in `server.listen_addr` — these examples use
`:5433`; the Docker guide uses `:5432`. Match your own config.)

PgFox authenticates `myrole` with SCRAM-SHA-256 (verifying against the role's
stored password in PostgreSQL), then services the session by borrowing backend
connections authenticated with a certificate for `myrole`. The `dbname` and
`user` choose which target and pool are used.

A `deny` rule (such as the sample's block on `template0`/`template1`) causes
matching connections to be rejected.

### Connecting over TLS

The hop above uses `sslmode=disable`, which secures nothing at the transport
level but still runs SCRAM. If you want the client↔PgFox link encrypted too,
PgFox accepts TLS: set `server.hostname` to the host clients connect to, give
clients the CA (`ca.crt`), and connect with a verifying SSL mode:

```bash
psql "host=pgfox.example.com port=5433 dbname=mydb user=myrole \
      sslmode=verify-full sslrootcert=ca.crt"
```

PgFox upgrades the connection on the client's `SSLRequest` using
`{pgfox_dir}/pgfox.crt`; clients that don't request TLS continue in plaintext.

## Regular queries (autocommit)

```sql
SELECT * FROM users WHERE active;
INSERT INTO audit_log (event) VALUES ('login');
```

Each statement borrows an idle backend, runs, and the backend is returned to the
pool as soon as it reports idle. Consecutive statements may run on different
backends. This is what lets a large number of mostly-idle clients share a small
pool.

## Transactions

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

On `BEGIN`, PgFox pins a backend to the session and keeps it for every statement
until `COMMIT` or `ROLLBACK`, after which the backend returns to the pool. The
whole transaction runs on one consistent backend, so isolation and locking
behave exactly as PostgreSQL intends. A failed transaction (status `E`) stays
pinned until you `ROLLBACK`.

If the client disconnects mid-transaction, PgFox terminates that backend instead
of returning it to the pool, so a half-finished transaction never leaks to
another client.

## Prepared statements

PgFox supports both the extended-protocol prepared statements used by drivers
like **asyncpg** and the server-side `PREPARE` used by drivers like **psycopg2**,
and shares them across pooled connections.

### Driver-managed (asyncpg)

asyncpg issues `Parse`/`Bind`/`Execute` with generated statement names. PgFox
rewrites those to content-addressed internal names (`pfx_<hash>`), so two
clients preparing the same query share one parsed statement on the backend. The
statement is parsed once per backend and reused thereafter.

```python
# asyncpg — works unchanged through PgFox
conn = await asyncpg.connect(host="localhost", port=5433,
                             user="myrole", database="mydb")
rows = await conn.fetch("SELECT * FROM orders WHERE status = $1", "open")
```

### Simple queries with literals

A plain query that contains literal values can also be parameterized and shared:
PgFox extracts the literals, canonicalizes the SQL, and routes it through the
same `pfx_` cache. So even clients that do not explicitly prepare benefit from
shared, pre-parsed statements.

### When a statement pins the backend

Some prepared statements cannot be shared safely — their state is specific to one
connection. PgFox keeps those on their original name and pins the backend to the
client while they exist. This is automatic; the client sees normal prepared
statement behavior either way.

## LISTEN / NOTIFY

```sql
-- Session A
LISTEN order_updates;

-- Session B
NOTIFY order_updates, 'order 12345 shipped';

-- Session A receives the notification asynchronously
```

PgFox keeps one shared monitor connection per channel and fans each notification
out to every subscribed client, rather than dedicating a backend connection to
each listener. Listeners receive notifications in real time, and the last client
to `UNLISTEN` (or disconnect) tears the monitor down.

Transactional semantics are preserved:

```sql
BEGIN;
LISTEN inventory;     -- buffered, not yet active
NOTIFY inventory;     -- queued
COMMIT;               -- now the LISTEN takes effect and the NOTIFY is delivered
```

`LISTEN`/`UNLISTEN` issued inside a transaction take effect only on `COMMIT` and
are discarded on `ROLLBACK`; a `NOTIFY` inside a transaction is delivered at
`COMMIT`. Issuing them in a failed transaction is rejected, as in PostgreSQL.

## Cancelling a query

Query cancellation works through PgFox the same way it does against PostgreSQL —
press Ctrl-C in `psql`, or call the driver's cancel API:

```sql
SELECT pg_sleep(30);   -- press Ctrl-C
-- ERROR: canceling statement due to user request
```

PgFox routes the cancel to whichever backend is currently running that client's
query (see [protocol.md](protocol.md#cancellation-out-of-band)). Cancellation is
asynchronous, so a query that finishes in the instant before the cancel arrives
simply completes.

## Monitoring

When `metrics.enabled` is true, PgFox serves Prometheus-format metrics at
`/metrics` on the configured metrics port (`4503` in the sample config):

```bash
curl http://localhost:4503/metrics
```

Exported metrics include:

| Metric | Type | Meaning |
|--------|------|---------|
| `pgfox_clients_total` | counter | Total client connections accepted. |
| `pgfox_clients_active` | gauge | Currently active client connections. |
| `pgfox_pools` | gauge | Number of pools. |
| `pgfox_queries_total` | counter | Total queries executed. |
| `pgfox_notifications_sent_total` | counter | Notifications fanned out to clients. |
| `pgfox_idle_connections_closed_total` | counter | Idle backend connections reaped. |
| `pgfox_target_connections_total` | gauge | Open backend connections per target. |

## Reloading configuration without downtime

Edit `config.yaml` and send `SIGHUP`:

```bash
kill -HUP "$(pgrep pgfox)"
```

PgFox parses the new file first and keeps the old configuration if parsing fails.
On success it applies the new settings, swapping the client listener if the
listen address changed — existing connections are not dropped to pick up the new
config.
