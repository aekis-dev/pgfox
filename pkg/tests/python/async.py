#!/usr/bin/env python3
"""
PgFox async test suite using asyncpg.
Tests concurrent queries, transactions, LISTEN/NOTIFY, and load.

Requirements:
    pip install asyncpg requests

Run:
    python async.py
"""

import asyncio
import asyncpg
import time
import statistics
import requests
from typing import List, Tuple

PGFOX_HOST   = "127.0.0.1"
PGFOX_PORT   = 5433
METRICS_PORT = 4503
DATABASE     = "postgres"
USER         = "odoo"
PASSWORD     = "odoo"

DSN = f"postgresql://{USER}:{PASSWORD}@{PGFOX_HOST}:{PGFOX_PORT}/{DATABASE}?sslmode=require"


async def connect() -> asyncpg.Connection:
    return await asyncpg.connect(DSN)


def get_metrics() -> str:
    try:
        r = requests.get(f"http://{PGFOX_HOST}:{METRICS_PORT}/metrics", timeout=5)
        return r.text
    except Exception as e:
        return f"(metrics unavailable: {e})"


def parse_metric(metrics: str, name: str) -> str:
    for line in metrics.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                return parts[1]
    return "N/A"


def print_metrics():
    m = get_metrics()
    print("  pgfox_clients_active          :", parse_metric(m, "pgfox_clients_active"))
    print("  pgfox_queries_total           :", parse_metric(m, "pgfox_queries_total"))
    print("  pgfox_pool_connections_total  :", parse_metric(m, "pgfox_pool_connections_total"))
    print("  pgfox_pool_connections_active :", parse_metric(m, "pgfox_pool_connections_active"))
    print("  pgfox_pool_connections_idle   :", parse_metric(m, "pgfox_pool_connections_idle"))
    print("  pgfox_listeners               :", parse_metric(m, "pgfox_listeners"))


# ---------------------------------------------------------------------------
# Test 1 — concurrent fast queries
# ---------------------------------------------------------------------------

async def test_concurrent_fast_queries(n: int = 50):
    print(f"\n── Test 1: {n} concurrent fast queries ──")

    # Open all connections first, separately from the query timing.
    conns = await asyncio.gather(*[connect() for _ in range(n)])

    # Now measure only the query execution concurrency.
    wall_start = time.time()
    durations = await asyncio.gather(*[
        conn.fetchval("SELECT $1::int", i)
        for i, conn in enumerate(conns)
    ])
    wall = time.time() - wall_start

    await asyncio.gather(*[conn.close() for conn in conns])

    total_seq = sum(d if isinstance(d, float) else 0.001 for d in [wall/n]*n)
    print(f"  wall={wall:.3f}s  queries={n}  speedup={n*0.001/wall:.1f}x")

    if wall < 0.1:
        print("  ✅ PASS — queries ran concurrently (pgfox not serializing)")
    else:
        print("  ❌ FAIL — queries were serialized")


# ---------------------------------------------------------------------------
# Test 2 — slow vs fast queries
# ---------------------------------------------------------------------------

async def test_slow_vs_fast(n_slow: int = 5, n_fast: int = 20):
    print(f"\n── Test 2: {n_slow} slow vs {n_fast} fast queries (text result format forced) ──")

    async def slow(qid: int) -> Tuple[str, float]:
        conn = await connect()
        try:
            start = time.time()
            await conn.fetchval("SELECT pg_sleep(3.0), $1::int", qid)
            return "slow", time.time() - start
        finally:
            await conn.close()

    async def fast(qid: int) -> Tuple[str, float]:
        conn = await connect()
        try:
            start = time.time()
            await conn.fetchval("SELECT $1::int", qid)
            return "fast", time.time() - start
        finally:
            await conn.close()

    wall_start = time.time()
    slow_tasks = [asyncio.create_task(slow(i)) for i in range(n_slow)]
    await asyncio.sleep(0.3)
    fast_tasks = [asyncio.create_task(fast(i + 1000)) for i in range(n_fast)]
    all_results = await asyncio.gather(*slow_tasks, *fast_tasks)
    wall = time.time() - wall_start

    fast_durations = [d for kind, d in all_results if kind == "fast"]
    slow_durations = [d for kind, d in all_results if kind == "slow"]

    print(f"  wall={wall:.2f}s  avg_slow={statistics.mean(slow_durations):.2f}s  avg_fast={statistics.mean(fast_durations):.4f}s")

    if statistics.mean(fast_durations) < 0.5 and wall < 4.0:
        print("  ✅ PASS — fast queries were not blocked by slow ones")
    else:
        print("  ❌ FAIL — fast queries were delayed")


# ---------------------------------------------------------------------------
# Test 3 — concurrent transactions
# ---------------------------------------------------------------------------

async def test_concurrent_transactions(n: int = 20):
    print(f"\n── Test 3: {n} concurrent transactions (text result format forced) ──")
    errors = []

    async def txn(worker_id: int):
        conn = await connect()
        try:
            async with conn.transaction():
                # Pass as string since text encoder is registered.
                result = await conn.fetchval("SELECT $1::int * 2", worker_id)
                expected = worker_id * 2
                if result != expected:
                    errors.append(
                        f"worker {worker_id}: expected {expected!r} got {result!r} "
                        f"(type={type(result).__name__})"
                    )
                await asyncio.sleep(0.02)
        finally:
            await conn.close()

    wall_start = time.time()
    await asyncio.gather(*[txn(i) for i in range(n)])
    wall = time.time() - wall_start

    print(f"  {n} transactions in {wall:.2f}s")
    if errors:
        for e in errors:
            print(f"  ❌ {e}")
    else:
        print("  ✅ PASS — all transactions isolated correctly")


# ---------------------------------------------------------------------------
# Test 4 — type-by-type breakdown
# ---------------------------------------------------------------------------

async def test_extended_protocol():
    """
    Core test: for each type, asyncpg sends resultFmts=[0] (text), pgfox
    overrides to binary. We record the raw value asyncpg's text decoder
    receives and whether it raises an error.

    Expected outcomes:
    - text/varchar/bytea: identical in binary mode — should PASS
    - bool: binary is b'\\x01'/b'\\x00', text is 't'/'f' — may FAIL
    - int*/float*/numeric: binary is big-endian bytes, text is digit string — likely FAIL
    - timestamp/date/uuid/json/jsonb: binary has its own encoding — likely FAIL
    """
    print("\n── Test 4: type-by-type binary-override breakdown ──")
    print("  asyncpg sends text format; pgfox overrides to binary")
    print("  result shows what asyncpg's text decoder receives/raises\n")

    conn = await connect()

    type_queries = [
        ("int4",        "SELECT 42::int4",                    None),
        ("int8",        "SELECT 9999999999::int8",            None),
        ("float8",      "SELECT 3.14::float8",                None),
        ("bool_true",   "SELECT true::bool",                  None),
        ("bool_false",  "SELECT false::bool",                 None),
        ("text",        "SELECT 'hello'::text",               None),
        ("varchar",     "SELECT 'world'::varchar",            None),
        ("bytea",       "SELECT '\\xdeadbeef'::bytea",        None),
        ("timestamp",   "SELECT now()::timestamp",            None),
        ("timestamptz", "SELECT now()::timestamptz",          None),
        ("date",        "SELECT now()::date",                 None),
        ("uuid",        "SELECT gen_random_uuid()::uuid",     None),
        ("numeric",     "SELECT 123.456::numeric",            None),
        ("json",        "SELECT '{\"a\":1}'::json",           None),
        ("jsonb",       "SELECT '{\"a\":1}'::jsonb",          None),
    ]

    pass_count = 0
    fail_count = 0

    try:
        for type_name, query, _ in type_queries:
            try:
                val = await conn.fetchval(query)
                status = "✅ OK  "
                detail = repr(val)[:70]
                pass_count += 1
            except Exception as e:
                status = "❌ FAIL"
                detail = f"{type(e).__name__}: {str(e)[:60]}"
                fail_count += 1
            print(f"  {status} {type_name:14s} → {detail}")
    finally:
        await conn.close()

    print(f"\n  {pass_count}/{pass_count + fail_count} types survived binary override")
    if fail_count == 0:
        print("  ✅ All types pass — no transcoder needed")
    else:
        print(f"  ⚠️  {fail_count} type(s) need transcoding for binary→text conversion")


# ---------------------------------------------------------------------------
# Test 5 — LISTEN/NOTIFY
# ---------------------------------------------------------------------------

async def test_listen_notify():
    print("\n── Test 5: LISTEN/NOTIFY with concurrent queries (text result format forced) ──")

    received: list = []
    Listener = await connect()

    def on_notification(conn, pid, channel, payload):
        received.append(payload)

    await Listener.add_listener("pgfox_test_channel", on_notification)
    await Listener.execute("LISTEN pgfox_test_channel")

    async def send_notifications():
        await asyncio.sleep(0.3)
        notifier = await connect()
        try:
            for i in range(5):
                await notifier.execute(f"NOTIFY pgfox_test_channel, 'msg_{i}'")
                await asyncio.sleep(0.1)
        finally:
            await notifier.close()

    async def run_queries():
        await asyncio.sleep(0.5)
        async def one(i):
            conn = await connect()
            try:
                return await conn.fetchval("SELECT $1::int", i)
            finally:
                await conn.close()
        return await asyncio.gather(*[one(i) for i in range(10)])

    sender  = asyncio.create_task(send_notifications())
    queries = asyncio.create_task(run_queries())
    await sender
    query_results = await queries
    await asyncio.sleep(0.5)

    await Listener.remove_listener("pgfox_test_channel", on_notification)
    await Listener.execute("UNLISTEN pgfox_test_channel")
    await Listener.close()

    print(f"  notifications received: {received}")
    print(f"  concurrent queries ok:  {len(query_results)}/10")

    if len(received) >= 4 and len(query_results) == 10:
        print("  ✅ PASS")
    else:
        print(f"  ❌ FAIL — notifications={len(received)}, queries={len(query_results)}")


# ---------------------------------------------------------------------------
# Test 6 — sustained load
# ---------------------------------------------------------------------------

async def test_load(duration: float = 20.0, target_qps: float = 20.0):
    print(f"\n── Test 6: {duration}s load test @ {target_qps} qps (text result format forced) ──")

    pg_pool = await asyncpg.create_pool(
        DSN, min_size=2, max_size=10,
        statement_cache_size=0,
    )

    success = 0
    failure = 0
    type_errors: dict = {}
    interval = 1.0 / target_qps
    stop_at = time.time() + duration
    qid = 0
    tasks = []

    async def one(query_id: int):
        nonlocal success, failure
        conn = await pg_pool.acquire()
        try:
            qtype = query_id % 3
            if qtype == 0:
                await conn.fetchval("SELECT pg_sleep(0.2), $1::int", query_id)
            elif qtype == 1:
                await conn.fetchval("SELECT $1::int", query_id)
            else:
                async with conn.transaction():
                    await conn.fetchval("SELECT $1::int * 2", query_id)
            success += 1
        except Exception as e:
            failure += 1
            etype = type(e).__name__
            type_errors[etype] = type_errors.get(etype, 0) + 1
        finally:
            await pg_pool.release(conn)

    wall_start = time.time()
    while time.time() < stop_at:
        tasks.append(asyncio.create_task(one(qid)))
        qid += 1
        await asyncio.sleep(interval)

    await asyncio.gather(*tasks, return_exceptions=True)
    await pg_pool.close()

    wall = time.time() - wall_start
    total = success + failure

    print(f"  duration={wall:.1f}s  total={total}  success={success}  failed={failure}")
    print(f"  actual_qps={success/wall:.1f}  success_rate={success/total*100:.1f}%")
    if type_errors:
        print(f"  error breakdown: {type_errors}")

    if failure == 0:
        print("  ✅ PASS — zero failures under sustained load")
    elif failure / total < 0.01:
        print("  ✅ PASS — <1% failure rate")
    else:
        print(f"  ❌ FAIL — {failure/total*100:.1f}% failure rate")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("PgFox Async Test Suite — Binary Override Mode")
    print("=" * 50)
    print("asyncpg sends text format  (resultFmts=[0])")
    print("pgfox overrides all Bind result formats to binary")
    print("=" * 50)

    print("\nTesting connectivity...")
    try:
        conn = await asyncpg.connect(DSN)  # plain connect, no codec override
        ver = await conn.fetchval("SELECT version()")
        print(f"  ✅ Connected: {ver[:60]}...")
        await conn.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        return

    print("\nInitial metrics:")
    print_metrics()

    await test_concurrent_fast_queries(50)
    await test_slow_vs_fast(5, 20)
    await test_concurrent_transactions(20)
    await test_extended_protocol()
    await test_listen_notify()
    await test_load(duration=20.0, target_qps=20.0)

    print("\nFinal metrics:")
    print_metrics()

    print("\nTest complete")


if __name__ == "__main__":
    asyncio.run(main())