#!/usr/bin/env python3
"""
load.py -- run the generated SQL against a SQL Server via sqlcmd.

    python load.py deploy            create database if needed, run out/01..11 in order
    python load.py seed              run out/seed/*.sql (tenant config + staging data)
    python load.py run [--mode Full|Partial]   EXEC etl.Load_Warehouse, then show the run log
    python load.py query [--sql "..."] [--top 10]   default: sample data.StudentClass
    python load.py drop              run out/99_drop_all.sql
    python load.py all               deploy + seed + run + query

Connection: --server / --database / --user / --password (or env EDW_SERVER,
EDW_DATABASE, EDW_USER, EDW_PASSWORD). Requires sqlcmd on PATH.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def sqlcmd(args, database: str | None, *, query: str | None = None, file: Path | None = None, wide: bool = False) -> str:
    cmd = ["sqlcmd", "-S", args.server, "-U", args.user, "-P", args.password, "-C", "-b", "-I"]
    if database:
        cmd += ["-d", database]
    if wide:
        cmd += ["-W", "-s", " | "]
    if query is not None:
        cmd += ["-Q", query]
    else:
        cmd += ["-i", str(file)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise SystemExit(f"sqlcmd failed ({'file ' + str(file) if file else 'query'})")
    return res.stdout


def run_files(args, files: list[Path]) -> None:
    for f in files:
        out = sqlcmd(args, args.database, file=f)
        errors = [l for l in out.splitlines() if "Msg " in l]
        print(f"  ran {f}" + (f"\n{out}" if errors else ""))


def cmd_deploy(args) -> None:
    sqlcmd(args, "master", query=f"IF DB_ID('{args.database}') IS NULL CREATE DATABASE [{args.database}];")
    print(f"database {args.database} ready on {args.server}")
    run_files(args, sorted(p for p in Path(args.out).glob("[0-9][0-9]_*.sql") if not p.name.startswith("99")))


def cmd_seed(args) -> None:
    run_files(args, sorted(Path(args.out, "seed").glob("*.sql")))


def cmd_run(args) -> None:
    print(sqlcmd(args, args.database, wide=True, query=(
        f"EXEC etl.Load_Warehouse @RefreshMode = '{args.mode}';\n"
        "SELECT ProcessLogKey, ParentProcessLogKey AS Parent, ProcessName, StatusCode, "
        "DurationMs = DATEDIFF(ms, StartDateTime, EndDateTime), RowsInserted, RowsUpdated, RowsExpired, "
        "ErrorMessage = LEFT(ErrorMessage, 80) FROM control.ProcessLog "
        "WHERE ProcessLogKey >= (SELECT MAX(ProcessLogKey) FROM control.ProcessLog WHERE ProcessName = 'Load_Warehouse') "
        "ORDER BY ProcessLogKey;")))


def cmd_query(args) -> None:
    sql = args.sql or (
        f"SELECT TOP {args.top} StudentClassKey, StudentCode, StudentName, TermCode, ClassCode, CourseCode, "
        "ClassDeliveryTypeName, ProgramCode, RegistrationDate, WithdrawalDate, CreditHoursAttempted, CreditHoursEarned, "
        "GradePoints, CompletedCount, WithdrawnCount FROM data.StudentClass ORDER BY StudentClassKey;")
    print(sqlcmd(args, args.database, wide=True, query=sql))


def cmd_drop(args) -> None:
    run_files(args, [Path(args.out, "99_drop_all.sql")])


def cmd_all(args) -> None:
    cmd_deploy(args); cmd_seed(args); cmd_run(args); cmd_query(args)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["deploy", "seed", "run", "query", "drop", "all"])
    ap.add_argument("--server", default=os.environ.get("EDW_SERVER", "localhost,1433"))
    ap.add_argument("--database", default=os.environ.get("EDW_DATABASE", "EDW_Dev"))
    ap.add_argument("--user", default=os.environ.get("EDW_USER", "sa"))
    ap.add_argument("--password", default=os.environ.get("EDW_PASSWORD"))
    ap.add_argument("--out", default="out")
    ap.add_argument("--mode", default="Full", choices=["Full", "Partial"])
    ap.add_argument("--sql")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()
    if not args.password:
        raise SystemExit("password required: --password or EDW_PASSWORD")
    {"deploy": cmd_deploy, "seed": cmd_seed, "run": cmd_run, "query": cmd_query, "drop": cmd_drop, "all": cmd_all}[args.command](args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
