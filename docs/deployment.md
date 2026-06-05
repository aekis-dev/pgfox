# Deployment: PgFox + PostgreSQL with Docker

This guide gets PgFox and PostgreSQL talking to each other from scratch. It
covers the certificate authority they share, the PostgreSQL server-side
configuration that lets PgFox authenticate as any role, and how to wire it all
into a Compose deployment.

Read [architecture.md](architecture.md) first if you want the "why"; this
document is the "how".

## The trust model in one picture

PgFox and PostgreSQL share a single certificate authority (CA). Everything else
follows from that.

```
                 +----------------------------- one CA -----------------------------+
                 |  ca.crt (public)                         ca.key (private)         |
                 +---------------+----------------------------------+----------------+
                                 | signs                            | signs
         +-----------------------v----------+        +--------------v--------------+
         | PgFox                            |  TLS   | PostgreSQL                  |
         |  - signs a client cert per role  |------->|  - server cert (signed by   |
         |    (CN = role), presents it      |        |    the CA, SAN = host PgFox |
         |  - trusts the backend via ca.crt |        |    dials)                   |
         +----------------------------------+        |  - ssl_ca_file = ca.crt     |
                                                      |  - pg_hba: hostssl ... cert |
                                                      +-----------------------------+
```

Two consequences worth keeping in mind:

1. **PgFox verifies the backend hostname (verify-full).** PostgreSQL's *server*
   certificate must be signed by the shared CA and carry a SAN matching the
   `host` in PgFox's target config.
2. **PostgreSQL authenticates PgFox by client certificate.** PgFox presents a
   cert whose CN is the role name; PostgreSQL maps CN -> role. No passwords
   cross the wire to the backend.

You can satisfy both either by letting PgFox generate everything (Option A,
fastest) or by bringing your own CA (Option B).

## Option A - Let PgFox generate the certificates (fastest)

On its first run, PgFox generates a complete set of bootstrap certificates into
`pgfox_dir` and logs the files to copy to PostgreSQL. This is the quickest way
to get started, and the generated PostgreSQL server certificate already has the
right SAN - its CN/SAN is set to your **first target's host**, which is exactly
the name PgFox dials, so backend verification just works.

Files generated under `{pgfox_dir}`:

| File | Purpose | Who needs it |
|------|---------|--------------|
| `ca.crt` | The shared CA certificate | PgFox **and** PostgreSQL |
| `ca.key` | The CA private key (signs all other certs) | PgFox only - keep it here |
| `server.crt` / `server.key` | PostgreSQL **server** cert (CN/SAN = first target host) | PostgreSQL |
| `pgfox.crt` / `pgfox.key` | PgFox's own client-facing TLS cert | PgFox only |
| `certs/{role}.crt` / `.key` | Per-role client certs, generated on demand | PgFox only |

### 1. Run PgFox once to generate the certs

Use a persistent volume for `pgfox_dir` so the generated CA survives restarts
and you can read the files out of it:

```bash
docker volume create pgfox-data

docker run --rm \
  -v pgfox-data:/var/lib/pgfox/data \
  -v "$PWD/config.yaml:/etc/pgfox/config.yaml:ro" \
  aekis/pgfox:latest -config /etc/pgfox/config.yaml
```

PgFox writes the certificates immediately and logs a line like
`PostgreSQL server setup - copy these files to $PGDATA`. It then tries its
privileged backend connection, which **will fail on this first run** because
PostgreSQL does not trust the CA yet - that is expected. Stop the container once
the certificates have been generated.

### 2. Copy the CA and server cert to PostgreSQL

PostgreSQL needs three files - the CA certificate and the server key pair. It
does **not** need `ca.key` (that is the CA's private key; it stays with PgFox):

```bash
docker run --rm -v pgfox-data:/data alpine cat /data/ca.crt     > ca.crt
docker run --rm -v pgfox-data:/data alpine cat /data/server.crt > server.crt
docker run --rm -v pgfox-data:/data alpine cat /data/server.key > server.key
```

Then continue with [Configure PostgreSQL](#configure-postgresql) below.

When you run the real deployment, keep mounting the same `pgfox-data` volume so
PgFox reuses the CA it generated rather than making a new one.

## Option B - Bring your own CA

Use this when you want to control the CA yourself, or supply it inline via
Compose configs (as in the sample `compose.yaml`).

### 1. Create the CA

```bash
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 \
  -subj "/O=PgFox/OU=PostgreSQL Users/CN=PgFox Root CA" \
  -out ca.crt
```

### 2. Create PostgreSQL's server certificate

The SAN must match the `host` PgFox dials (in the sample Compose that is the
service alias `3b528289-a9c2-4054-8bf0-7d6108ac5d54`):

```bash
HOST=3b528289-a9c2-4054-8bf0-7d6108ac5d54
openssl genrsa -out server.key 4096
openssl req -new -key server.key -subj "/CN=${HOST}" -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -sha256 \
  -extfile <(printf "subjectAltName=DNS:%s" "${HOST}") \
  -out server.crt
chmod 600 server.key
```

Use `subjectAltName=IP:1.2.3.4` if PgFox connects by IP.

### 3. Give the CA to PgFox

PgFox expects the CA at `{pgfox_dir}/ca.crt` and `{pgfox_dir}/ca.key`; when both
exist it uses them as-is and never regenerates them. The sample `compose.yaml`
supplies them as inline configs mounted into `pgfox_dir`:

```yaml
configs:
  pgfox_ca_crt:
    content: |
      -----BEGIN CERTIFICATE-----
      ...paste ca.crt...
      -----END CERTIFICATE-----
  pgfox_ca_key:
    content: |
      -----BEGIN PRIVATE KEY-----
      ...paste ca.key...
      -----END PRIVATE KEY-----
```

> **Security:** Swarm configs are stored unencrypted in the raft log, which is
> wrong for a private key. For production, keep `ca.crt` as a config but move
> `ca.key` to a Docker **secret** (encrypted at rest), mounted at
> `/var/lib/pgfox/data/ca.key`:
>
> ```yaml
> services:
>   pgfox:
>     secrets:
>       - source: pgfox_ca_key
>         target: /var/lib/pgfox/data/ca.key
>         mode: 0400
> secrets:
>   pgfox_ca_key:
>     file: ./ca.key        # keep out of version control
> ```

## Configure PostgreSQL

This part is identical for both options - you have a `ca.crt`, `server.crt`, and
`server.key` from whichever route you took.

### `postgresql.conf`

```conf
ssl = on
ssl_ca_file   = 'ca.crt'        # the SHARED CA
ssl_cert_file = 'server.crt'
ssl_key_file  = 'server.key'
```

Place the files where these paths resolve (commonly `$PGDATA`), and ensure
`server.key` is owned by the postgres user with mode `600`.

### `pg_hba.conf`

The `cert` method verifies the client certificate against `ssl_ca_file` and uses
the certificate CN as the role - which is what lets PgFox connect as any role.
Add a single `hostssl ... cert clientcert=verify-full` rule:

```conf
# pg_hba.conf entries for PgFox
#
# PgFox authenticates with a TLS client certificate whose CN is the role name.
# The `cert` method verifies that certificate against ssl_ca_file (the CA shared
# with PgFox) and maps the certificate CN to the PostgreSQL role, so this single
# rule lets PgFox connect as ANY role, including the privileged pgfox_role.
#
# TYPE  DATABASE  USER  ADDRESS        METHOD
hostssl    all       all   192.168.65.1/32    cert   clientcert=verify-full

# Keep whatever local/administrative entries you already rely on, e.g.:
# local    all        all                       peer
# host     all        all    127.0.0.1/32       scram-sha-256
```

(With the `cert` method `clientcert=verify-full` is implied; stating it is just
explicit.) This one rule covers every client role plus the privileged
`pgfox_role`, since all their certificates are signed by the trusted CA with the
role as CN.

The important column is **ADDRESS** - it must be the source address PostgreSQL
sees PgFox connecting *from*, not PgFox's own listen address. Depending on how
you run things:

- **PgFox in Docker Desktop, PostgreSQL on the host:** PgFox's traffic arrives
  via the Docker Desktop host gateway, so use `192.168.65.1/32` (the value in
  the example above).
- **PgFox and PostgreSQL on the same Docker network:** use that network's
  subnet, e.g. `10.0.1.0/24`.
- **PgFox on a known host:** that host's address as a `/32`.

If unsure, check PostgreSQL's log after a failed attempt - it reports the source
IP of the rejected connection, which is exactly the address to authorize.

### The privileged role

PgFox reads each role's SCRAM verifier from `pg_authid` to authenticate clients,
so `pgfox_role` needs permission to do that:

```sql
-- PostgreSQL 14+
CREATE ROLE odoo LOGIN;
GRANT pg_read_all_auth_data TO odoo;

-- PostgreSQL 13 and earlier: pgfox_role must be a superuser
-- ALTER ROLE odoo SUPERUSER;
```

Each application role your clients log in as must also exist with a SCRAM
password - that is what PgFox verifies the client against:

```sql
SET password_encryption = 'scram-sha-256';
CREATE ROLE myrole LOGIN PASSWORD 'the-client-password';
```

Reload PostgreSQL (`SELECT pg_reload_conf();` for `pg_hba.conf`; a restart for
the `ssl` settings). If PgFox was already running under Option A and failing its
backend connection, it now succeeds - restart it to retry immediately.

## Deploy and connect

```bash
docker stack deploy -c compose.yaml pgfox     # Swarm
# or: docker compose -f compose.yaml up -d    # single host
```

Connect a client through PgFox (port `5432` in the sample Compose):

```bash
psql "host=<pgfox-host> port=5432 dbname=mydb user=myrole sslmode=disable"
```

`sslmode=disable` refers to the *client -> PgFox* hop, which is secured by
SCRAM; PgFox -> PostgreSQL always uses TLS with the certificates above.

## A self-contained local example

Generate the certs with Option A (or Option B using `HOST=postgres`), drop
`ca.crt`, `server.crt`, `server.key`, and a `pg_hba.conf` next to this file,
then `docker compose up`.

Two things to get right for this example specifically:

- **Order matters.** PostgreSQL starts with `ssl=on` and needs `server.crt`, and
  PgFox needs the CA in its volume — so generate the certificates *first*, then
  `docker compose up`. (With Option A: run PgFox once against this config to mint
  the certs, copy `ca.crt`/`server.crt`/`server.key` out, then bring the stack
  up.)
- **The `pg_hba.conf` here replaces PostgreSQL's default.** Because the postgres
  service sets `hba_file`, your file must also contain the entries the container
  needs for its own startup, not just the PgFox rule. Uncomment the
  `local`/`host` lines from the sample above so local and loopback connections
  still work, e.g.:

  ```conf
  local      all   all                    trust
  host       all   all   127.0.0.1/32     trust
  hostssl    all   all   0.0.0.0/0        cert   clientcert=verify-full
  ```

  (`0.0.0.0/0` is fine on an isolated local network; tighten it elsewhere.)

```yaml
name: pgfox-local
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: postgres
    command:
      - postgres
      - -c
      - ssl=on
      - -c
      - ssl_cert_file=/certs/server.crt
      - -c
      - ssl_key_file=/certs/server.key
      - -c
      - ssl_ca_file=/certs/ca.crt
      - -c
      - hba_file=/certs/pg_hba.conf
    volumes:
      - ./server.crt:/certs/server.crt:ro
      - ./server.key:/certs/server.key:ro
      - ./ca.crt:/certs/ca.crt:ro
      - ./pg_hba.conf:/certs/pg_hba.conf:ro
    networks: [pgnet]

  pgfox:
    image: aekis/pgfox:latest
    ports:
      - "5432:5432"
      - "4502:4502"
    configs:
      - { source: pgfox_config, target: /etc/pgfox/config.yaml, mode: 0444 }
    volumes:
      - pgfox-data:/var/lib/pgfox/data   # PgFox generates the CA here (Option A)
    depends_on: [postgres]
    networks: [pgnet]

networks:
  pgnet: {}
volumes:
  pgfox-data: {}
configs:
  pgfox_config:
    content: |
      server:
        listen_addr: ":5432"
        pgfox_dir: /var/lib/pgfox/data
        pgfox_role: postgres
      targets:
        - name: local
          host: postgres        # = the server cert SAN and the service name
          port: 5432
      metrics: { enabled: true, port: 4502 }
```

With Option A, point `host` at `postgres`, run PgFox once to mint a `server.crt`
whose SAN is `postgres`, hand that plus `ca.crt` to the postgres service, and
everything lines up.

## Troubleshooting

| Symptom | Likely cause |
|---------|--------------|
| PgFox exits immediately on start | Provided `ca.crt`/`ca.key` are placeholder text or an invalid PEM pair (Option B). |
| First-run backend connection fails | Expected before PostgreSQL trusts the CA - finish the PostgreSQL setup and restart PgFox. |
| `x509: certificate signed by unknown authority` (PgFox -> backend) | PostgreSQL's server cert is not signed by the shared CA, or `ssl_ca_file` is not that CA. |
| `x509: certificate is valid for X, not Y` | The server cert SAN does not match the target `host`. With Option A, set `host` to the value you want *before* generating, since the SAN follows the first target host. |
| Backend: `connection requires a valid client certificate` | `pg_hba.conf` is not `hostssl ... cert`, or `ssl_ca_file` is not the shared CA. |
| Client auth fails for a valid password | `pgfox_role` lacks `pg_read_all_auth_data` (or is not superuser on PG <= 13). |
| Permission denied writing certs | `pgfox_dir` is not writable by the non-root `pgfox` user - see the Dockerfile `chown`. |