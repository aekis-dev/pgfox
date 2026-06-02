#!/usr/bin/env python3
"""
PgFox simple threaded test suite.
Tests concurrent queries, transactions, and connection Pool behavior.

Requirements:
    pip install psycopg2-binary requests

Run:
    python simple.py
"""

import psycopg2
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

PGFOX_HOST    = "127.0.0.1"
PGFOX_PORT    = 5433
METRICS_PORT  = 4503
DATABASE      = "deploy-soltein"
USER          = "odoo"
PASSWORD      = "odoo"

CONN_PARAMS = {
    "host":     PGFOX_HOST,
    "port":     PGFOX_PORT,
    "dbname":   DATABASE,
    "user":     USER,
    "password": PASSWORD,
    "sslmode":  "require",
    "connect_timeout": 10,
}


def connect():
    """Open and return a new psycopg2 connection."""
    return psycopg2.connect(**CONN_PARAMS)


def get_metrics() -> str:
    """Fetch Prometheus metrics from PgFox."""
    try:
        r = requests.get(f"http://{PGFOX_HOST}:{METRICS_PORT}/metrics", timeout=5)
        return r.text
    except Exception as e:
        return f"(metrics unavailable: {e})"


def parse_metric(metrics: str, name: str) -> str:
    """Extract the value of a specific metric from Prometheus text output."""
    for line in metrics.splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                return parts[1]
    return "N/A"


def print_metrics():
    """Print a summary of the key PgFox metrics."""
    m = get_metrics()
    print("  pgfox_clients_active          :", parse_metric(m, "pgfox_clients_active"))
    print("  pgfox_queries_total           :", parse_metric(m, "pgfox_queries_total"))
    print("  pgfox_pool_connections_total  :", parse_metric(m, "pgfox_pool_connections_total"))
    print("  pgfox_pool_connections_active :", parse_metric(m, "pgfox_pool_connections_active"))
    print("  pgfox_pool_connections_idle   :", parse_metric(m, "pgfox_pool_connections_idle"))


# ---------------------------------------------------------------------------
# Test 1 — fast vs slow queries
# ---------------------------------------------------------------------------

def fast_query(query_id: int) -> Tuple[str, int, float]:
    start = time.time()
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT %s::int", (query_id,))
            cur.fetchone()
    finally:
        conn.close()
    return "fast", query_id, time.time() - start


def slow_query(query_id: int, sleep_seconds: float = 3.0) -> Tuple[str, int, float]:
    start = time.time()
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_sleep(%s), %s::int", (sleep_seconds, query_id))
            cur.fetchone()
    finally:
        conn.close()
    return "slow", query_id, time.time() - start


def test_fast_vs_slow():
    """Fast queries must not be blocked by slow ones running in parallel."""
    print("\n── Test 1: fast vs slow queries ──")

    wall_start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = []
        # 3 slow queries (3s each)
        for i in range(3):
            futures.append(ex.submit(slow_query, i, 3))
        # slight pause then 10 fast queries
        time.sleep(0.3)
        for i in range(10):
            futures.append(ex.submit(fast_query, i + 100))

        for f in as_completed(futures):
            kind, qid, dur = f.result()
            elapsed = time.time() - wall_start
            results.append((kind, qid, dur, elapsed))
            print(f"  {elapsed:5.2f}s  {kind:4s}  id={qid:3d}  took={dur:.3f}s")

    fast_results = [r for r in results if r[0] == "fast"]
    early = sum(1 for r in fast_results if r[3] < 2.0)

    print(f"\n  Fast queries completed before 2s: {early}/{len(fast_results)}")
    if early >= 8:
        print("  ✅ PASS — fast queries ran concurrently with slow ones")
    else:
        print("  ❌ FAIL — fast queries were delayed by slow ones")


# ---------------------------------------------------------------------------
# Test 2 — concurrent connections
# ---------------------------------------------------------------------------

def test_concurrent_connections():
    """Many connections running simple queries simultaneously."""
    print("\n── Test 2: concurrent connections ──")

    def worker(worker_id: int) -> List[Tuple]:
        results = []
        conn = connect()
        try:
            for i in range(5):
                start = time.time()
                with conn.cursor() as cur:
                    cur.execute("SELECT %s::int, %s::int", (worker_id, i))
                    cur.fetchone()
                results.append((worker_id, i, time.time() - start))
        finally:
            conn.close()
        return results

    start = time.time()
    all_results = []

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(worker, i) for i in range(10)]
        for f in as_completed(futures):
            all_results.extend(f.result())

    total = time.time() - start
    print(f"  {len(all_results)} queries across 10 connections in {total:.2f}s")
    print(f"  avg per query: {total/len(all_results):.3f}s")
    print("  ✅ PASS")


# ---------------------------------------------------------------------------
# Test 3 — transactions
# ---------------------------------------------------------------------------

def test_transactions():
    """Concurrent transactions using a temp table per connection."""
    print("\n── Test 3: concurrent transactions ──")

    def txn_worker(worker_id: int) -> Tuple[int, int, float]:
        start = time.time()
        conn = connect()
        try:
            with conn.cursor() as cur:
                # Each connection gets its own temp table (scoped to the session)
                cur.execute(
                    "CREATE TEMP TABLE txn_test (id INT, value TEXT) ON COMMIT DROP"
                )
                cur.execute("BEGIN")
                cur.execute(
                    "INSERT INTO txn_test VALUES (%s, %s)",
                    (worker_id, f"worker_{worker_id}")
                )
                cur.execute("SELECT COUNT(*) FROM txn_test")
                count = cur.fetchone()[0]
                cur.execute("COMMIT")
        finally:
            conn.close()
        return worker_id, count, time.time() - start

    start = time.time()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(txn_worker, i) for i in range(5)]
        results = [f.result() for f in as_completed(futures)]

    total = time.time() - start
    print(f"  {len(results)} transactions in {total:.2f}s")

    all_ok = all(count == 1 for _, count, _ in results)
    for wid, count, dur in results:
        print(f"  worker {wid}: {count} row(s), {dur:.3f}s")

    if all_ok:
        print("  ✅ PASS — all transactions committed correctly")
    else:
        print("  ❌ FAIL — unexpected row count")


# ---------------------------------------------------------------------------
# Test 4 — extended query protocol
# ---------------------------------------------------------------------------

def test_extended_protocol():
    """
    psycopg2 uses the extended protocol (Parse/Bind/Execute) when
    executemany or prepared statements are used explicitly.
    We test parameterized queries which go through extended protocol.
    """
    print("\n── Test 4: extended query protocol ──")

    conn = connect()
    try:
        with conn.cursor() as cur:
            # Parameterized query — uses extended protocol
            cur.execute("SELECT %s::int + %s::int", (3, 4))
            result = cur.fetchone()[0]
            assert result == 7, f"expected 7, got {result}"

            # Multi-row parameterized result
            cur.execute("SELECT n FROM generate_series(1, %s) n", (5,))
            rows = cur.fetchall()
            assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

            # Error handling — connection must be usable after error
            try:
                cur.execute("SELECT 1/0")
                cur.fetchone()
            except psycopg2.errors.DivisionByZero:
                conn.rollback()

            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

        print("  ✅ PASS — parameterized queries, multi-row, error recovery")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Test 5 — transaction pinning under load
# ---------------------------------------------------------------------------

def test_transaction_pinning():
    """
    Transactions must stay pinned to the same backend connection.
    Interleave transactions from multiple workers — each must see
    only its own data.
    """
    print("\n── Test 5: transaction pinning under load ──")
    errors = []

    def pinned_worker(worker_id: int):
        conn = connect()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                cur.execute("SELECT %s::int * 2", (worker_id,))
                expected = worker_id * 2
                got = cur.fetchone()[0]
                if got != expected:
                    errors.append(f"worker {worker_id}: expected {expected}, got {got}")
                # Hold transaction open briefly to create interleaving
                time.sleep(0.05)
                cur.execute("COMMIT")
        finally:
            conn.close()

    threads = [threading.Thread(target=pinned_worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        for e in errors:
            print(f"  ❌ {e}")
    else:
        print("  ✅ PASS — all 20 transactions saw correct values")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("PgFox Test Suite")
    print("=" * 50)

    print("\nTesting connectivity...")
    try:
        conn = connect()
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            ver = cur.fetchone()[0]
            print(f"  ✅ Connected: {ver[:60]}...")
        conn.close()
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        return

    print("\nInitial metrics:")
    print_metrics()

    test_fast_vs_slow()
    test_concurrent_connections()
    test_transactions()
    test_extended_protocol()
    test_transaction_pinning()

    print("\nFinal metrics:")
    print_metrics()

    print("\n✅ All tests complete")


if __name__ == "__main__":
    main()