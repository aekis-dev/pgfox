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
    """n queries fired concurrently — total time should be close to one query."""
    print(f"\n── Test 1: {n} concurrent fast queries ──")

    async def one(qid: int) -> float:
        conn = await connect()
        try:
            start = time.time()
            await conn.fetchval("SELECT $1::int", qid)
            return time.time() - start
        finally:
            await conn.close()

    wall_start = time.time()
    durations = await asyncio.gather(*[one(i) for i in range(n)])
    wall = time.time() - wall_start

    avg  = statistics.mean(durations)
    seq  = sum(durations)
    print(f"  wall={wall:.2f}s  avg={avg:.3f}s  sequential_estimate={seq:.1f}s  speedup={seq/wall:.1f}x")

    if wall < seq * 0.5:
        print("  ✅ PASS — queries ran concurrently")
    else:
        print("  ❌ FAIL — insufficient concurrency")


# ---------------------------------------------------------------------------
# Test 2 — slow vs fast queries
# ---------------------------------------------------------------------------

async def test_slow_vs_fast(n_slow: int = 5, n_fast: int = 20):
    """Fast queries must complete quickly even while slow ones are running."""
    print(f"\n── Test 2: {n_slow} slow vs {n_fast} fast queries ──")

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

    avg_fast = statistics.mean(fast_durations)
    avg_slow = statistics.mean(slow_durations)

    print(f"  wall={wall:.2f}s  avg_slow={avg_slow:.2f}s  avg_fast={avg_fast:.4f}s")

    if avg_fast < 0.5 and wall < 4.0:
        print("  ✅ PASS — fast queries were not blocked by slow ones")
    else:
        print("  ❌ FAIL — fast queries were delayed")


# ---------------------------------------------------------------------------
# Test 3 — concurrent transactions
# ---------------------------------------------------------------------------

async def test_concurrent_transactions(n: int = 20):
    """Each coroutine runs a transaction; all must see only their own data."""
    print(f"\n── Test 3: {n} concurrent transactions ──")
    errors = []

    async def txn(worker_id: int):
        conn = await connect()
        try:
            async with conn.transaction():
                result = await conn.fetchval(
                    "SELECT $1::int * 2", worker_id
                )
                expected = worker_id * 2
                if result != expected:
                    errors.append(f"worker {worker_id}: expected {expected} got {result}")
                # brief hold to create interleaving
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
# Test 4 — extended query protocol (parameterized)
# ---------------------------------------------------------------------------

async def test_extended_protocol():
    """asyncpg always uses the extended protocol — Parse/Bind/Execute."""
    print("\n── Test 4: extended query protocol ──")

    conn = await connect()
    try:
        # Parameterized arithmetic
        result = await conn.fetchval("SELECT $1::int + $2::int", 3, 4)
        assert result == 7, f"expected 7, got {result}"

        # Multi-row result
        rows = await conn.fetch("SELECT n FROM generate_series(1, $1) n", 5)
        assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

        # Error recovery — connection must survive a query error
        try:
            await conn.fetchval("SELECT 1/0")
        except asyncpg.DivisionByZeroError:
            pass

        result = await conn.fetchval("SELECT 1")
        assert result == 1

        # Pipelined batch (multiple statements, one round trip)
        async with conn.transaction():
            r1 = await conn.fetchval("SELECT $1::int", 10)
            r2 = await conn.fetchval("SELECT $1::text", "hello")
            r3 = await conn.fetchval("SELECT $1::int + $2::int", 3, 4)
        assert r1 == 10
        assert r2 == "hello"
        assert r3 == 7

        print("  ✅ PASS — parameterized, multi-row, error recovery, batch")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Test 5 — LISTEN/NOTIFY with concurrent queries
# ---------------------------------------------------------------------------

async def test_listen_notify():
    """LISTEN on one connection, NOTIFY from another, queries from others."""
    print("\n── Test 5: LISTEN/NOTIFY with concurrent queries ──")

    received: list = []
    ready = asyncio.Event()

    listener = await connect()

    def on_notification(conn, pid, channel, payload):
        received.append(payload)

    await listener.add_listener("pgfox_test_channel", on_notification)
    await listener.execute("LISTEN pgfox_test_channel")

    async def send_notifications():
        await asyncio.sleep(0.3)  # let listener settle
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

    sender    = asyncio.create_task(send_notifications())
    queries   = asyncio.create_task(run_queries())

    await sender
    query_results = await queries

    # Give notifications a moment to arrive via the listener connection
    await asyncio.sleep(0.5)

    await listener.remove_listener("pgfox_test_channel", on_notification)
    await listener.execute("UNLISTEN pgfox_test_channel")
    await listener.close()

    print(f"  notifications received: {received}")
    print(f"  concurrent queries ok:  {len(query_results)}/10")

    if len(received) >= 4 and len(query_results) == 10:
        print("  ✅ PASS — notifications delivered, queries unblocked")
    else:
        print(f"  ❌ FAIL — notifications={len(received)}, queries={len(query_results)}")


# ---------------------------------------------------------------------------
# Test 6 — sustained load
# ---------------------------------------------------------------------------

async def test_load(duration: float = 20.0, target_qps: float = 20.0):
    """Sustained load for `duration` seconds using a connection pool."""
    print(f"\n── Test 6: {duration}s load test @ {target_qps} qps ──")

    # Use a pool with bounded concurrency — matches real application behavior.
    pg_pool = await asyncpg.create_pool(
        DSN, min_size=2, max_size=10,
        statement_cache_size=0
    )

    success = 0
    failure = 0
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
            print(f"  query {query_id} failed: {e}")
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
    print("PgFox Async Test Suite")
    print("=" * 50)

    print("\nTesting connectivity...")
    try:
        conn = await connect()
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

    print("\n✅ All tests complete")


if __name__ == "__main__":
    asyncio.run(main())