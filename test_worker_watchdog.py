"""
Regression test for the 2026-11-16 worker freeze.

The worker's main loop sat in a Postgres call from 13:21 to 22:38 UTC while the
container reported RUNNING, so the Gmail scanner (which runs on that loop)
processed nothing for nine hours. Two guards came out of it:

  - database.get_engine() returns one shared engine with connect / keepalive /
    statement timeouts, instead of a fresh engine (and leaked connections) per call.
  - worker.py's watchdog exits the process when the loop stops cycling, unless
    a known-long job (a pre-foreclosure scan) is running within its budget.

No network, no Postgres, no Railway.

    python test_worker_watchdog.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

FAILS = []


def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{(' — ' + str(detail)) if detail else ''}")
    if not cond:
        FAILS.append(label)


def test_engine():
    print("\n=== Database engine ===")
    import database

    database._ENGINE = None
    database.DATABASE_URL = None          # SQLite path: no network
    e1, e2 = database.get_engine(), database.get_engine()
    check("get_engine() returns the same engine every call", e1 is e2)
    check("Postgres connects with a connect timeout",
          database.PG_CONNECT_ARGS.get("connect_timeout", 0) > 0, database.PG_CONNECT_ARGS)
    check("Postgres connections use TCP keepalives",
          database.PG_CONNECT_ARGS.get("keepalives") == 1, database.PG_CONNECT_ARGS)
    check("unacknowledged sends time out",
          database.PG_CONNECT_ARGS.get("tcp_user_timeout", 0) > 0, database.PG_CONNECT_ARGS)
    database._ENGINE = None


def test_watchdog():
    print("\n=== Main-loop watchdog ===")
    import worker

    base = worker.WATCHDOG_STALL_SECONDS
    check("a loop that cycled recently is fine",
          worker._watchdog_overdue(now=1000.0, heartbeat=990.0, budgets=[]) == 0)
    check("the 13:21-22:38 stall (9h17m) is caught",
          worker._watchdog_overdue(now=33420.0, heartbeat=0.0, budgets=[]) > 0)
    check("base budget covers run_full's 1800s subprocess",
          worker._watchdog_overdue(now=1800.0 + 60, heartbeat=0.0, budgets=[]) == 0, base)
    check("a running scan extends the allowance",
          worker._watchdog_overdue(now=2 * 3600.0, heartbeat=0.0, budgets=[3 * 3600]) == 0)
    check("but a scan past its own budget is still caught",
          worker._watchdog_overdue(now=4 * 3600.0, heartbeat=0.0, budgets=[3 * 3600]) > 0)

    seen = []

    @worker._long_job(123)
    def job():
        seen.append(list(worker._long_job_budgets))
        raise RuntimeError("boom")

    try:
        job()
    except RuntimeError:
        pass
    check("a long job registers its budget while running", seen == [[123]], seen)
    check("and releases it even when it raises", worker._long_job_budgets == [],
          worker._long_job_budgets)
    check("run_preforeclosure_scan is marked as a long job",
          hasattr(worker.run_preforeclosure_scan, "__wrapped__"))


if __name__ == "__main__":
    test_engine()
    test_watchdog()
    print(f"\n=== {'ALL PASS' if not FAILS else str(len(FAILS)) + ' FAILED: ' + ', '.join(FAILS)} ===")
    sys.exit(1 if FAILS else 0)
