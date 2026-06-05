# Configuration

PgFox is configured with a YAML file passed via `--config`. The file has four
top-level sections: `server`, `targets`, `logging`, and `metrics`.

The examples below are taken from the sample `config.yaml`.

## Full example

```yaml
server:
  listen_addr: ":5433"
  max_connections: 1000
  connect_timeout: 10s
  idle_timeout: 10m
  max_message_size: 268435456   # 256 MB
  query_timeout: 0s             # 0 = disabled
  pgfox_dir: /var/lib/pgfox/data
  pgfox_role: odoo
  certs:
    ttl: 2160h                  # 90 days
    organization: "PgFox"
    organizational_unit: "PostgreSQL Users"
    country: "US"

logging:
  level: "debug"
  format: "text"
  # file: "/var/log/pgfox/pgfox.log"

targets:
  - name: primary_cluster
    host: 127.0.0.1
    port: 5432
    max_connections: 20
    connect_timeout: 20s
    priority: 1
    rules:
      - action: deny
        databases:
          - "template0"
          - "template1"
    parameters:
      application_name: "pgfox"
      timezone: "UTC"
      statement_timeout: "0"
      idle_in_transaction_session_timeout: "0"
      tcp_keepalives_idle: "600"
      tcp_keepalives_interval: "30"
      tcp_keepalives_count: "3"

metrics:
  enabled: true
  port: 4503
```

## `server`

| Field | Type | Meaning |
|-------|------|---------|
| `listen_addr` | string | Address PgFox listens on for clients (e.g. `:5433`). |
| `max_connections` | int | Maximum number of simultaneous client connections. |
| `connect_timeout` | duration | Default timeout when establishing a backend connection. |
| `idle_timeout` | duration | How long a pooled backend may sit idle before being closed. |
| `max_message_size` | int | Largest single protocol message accepted, in bytes. Must be large enough for the biggest object clients send. Default `268435456` (256 MB). |
| `query_timeout` | duration | Maximum time to wait for a complete backend response. `0s` disables it. |
| `pgfox_dir` | string | PgFox data directory; certificate paths are derived from it (see below). |
| `pgfox_role` | string | PostgreSQL role used for the privileged connection that reads `pg_authid`. Must have `pg_read_all_auth_data` (PG14+) or be a superuser. |
| `certs` | block | Certificate generation settings (see below). |

A note on `max_message_size` and `query_timeout`: applications that store large
binary objects (for example Odoo storing attachments as `bytea`) send them in a
single `Bind` message, so `max_message_size` must accommodate the largest such
object, and a fixed `query_timeout` can prematurely abort a legitimate large
upload. Leaving `query_timeout` at `0s` is the safe default unless you have a
known upper bound.

### Certificate paths

PgFox derives all certificate paths from `pgfox_dir`:

```
{pgfox_dir}/ca.crt                       CA certificate
{pgfox_dir}/ca.key                       CA private key
{pgfox_dir}/certs/{pgfox_role}.crt/.key  privileged-connection certificate
{pgfox_dir}/certs/{username}.crt/.key    per-user backend certificates
```

The CA and the `pgfox_role` certificate are generated automatically if missing.
Per-user certificates are generated on demand the first time a given role
connects, and renewed automatically when they expire.

### `server.certs`

| Field | Type | Meaning |
|-------|------|---------|
| `ttl` | duration | Validity period for generated user certificates; they are auto-renewed on expiry. |
| `organization` | string | `O` field in generated certificate subjects. |
| `organizational_unit` | string | `OU` field in generated certificate subjects. |
| `country` | string | `C` field in generated certificate subjects. |

## `targets`

A list of PostgreSQL servers PgFox can route to. Because PgFox is a true
passthrough proxy, targets contain **no credentials** — the client's own
identity (via SCRAM, then a per-user certificate) is what authenticates to the
backend.

| Field | Type | Meaning |
|-------|------|---------|
| `name` | string | Identifier for the target (used in logs and metrics). |
| `host` | string | Backend PostgreSQL host. |
| `port` | int | Backend PostgreSQL port. |
| `max_connections` | int | Maximum backend connections PgFox will open to this target. |
| `connect_timeout` | duration | Connection timeout for this target (overrides the server default). |
| `priority` | int | Ordering hint when more than one target could serve a request. |
| `rules` | list | Access rules evaluated against the connecting client (see below). |
| `parameters` | map | `key: value` server parameters applied to backend sessions on this target. |

### `targets[].rules`

Each rule has an `action` of `allow` or `deny`, and optional match fields. A
rule matches a connection when all of its specified fields match.

| Field | Type | Meaning |
|-------|------|---------|
| `action` | `allow` \| `deny` | What to do when the rule matches. |
| `cidr` | list of strings | Client source networks the rule applies to. |
| `users` | list of strings | Roles the rule applies to. |
| `databases` | list of strings | Databases the rule applies to. |

In the sample config a single `deny` rule blocks the `template0` and `template1`
databases; everything else is permitted.

### `targets[].parameters`

These become session parameters on backend connections — the same settings you
might otherwise put in `postgresql.conf` or send as startup options. The sample
config sets `application_name`, `timezone`, disables `statement_timeout` and
`idle_in_transaction_session_timeout`, and tunes TCP keepalives so dead peers
are detected.

## `logging`

| Field | Type | Meaning |
|-------|------|---------|
| `level` | string | Log verbosity (e.g. `debug`, `info`). |
| `format` | string | `text` or structured output. |
| `file` | string | Optional log file path; logs go to standard output if omitted. |

## `metrics`

| Field | Type | Meaning |
|-------|------|---------|
| `enabled` | bool | Whether to start the metrics HTTP server. |
| `port` | int | Port the metrics server listens on. |
| `path` | string | Optional path override for the metrics endpoint. |

The metrics server exposes Prometheus-format metrics at `/metrics`. See
[Usage scenarios](usage.md#monitoring) for the exported metric names.

## Reloading configuration

PgFox reloads its configuration on `SIGHUP` without restarting:

```bash
kill -HUP "$(pgrep pgfox)"
```

The new file is parsed first; if it fails to parse, the running configuration is
kept and the error is logged. On success the new configuration is applied,
including swapping the client listener if the listen address changed.
