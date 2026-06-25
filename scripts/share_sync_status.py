#!/usr/bin/python3
"""Read-only share-sync/download status report.

This script intentionally uses /usr/bin/python3 because the user's pyenv
Python may not include the sqlite3 extension, and PATH may contain an Android
sqlite3 binary that cannot run on this host.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


OK_STATUSES = {
    "completed",
    "skipped",
    "transferred",
}

PROBLEM_STATUSES = {
    "completed_with_errors",
    "completedwitherrors",
    "failed",
    "download_failed",
    "downloadfailed",
    "transfer_failed",
    "transferfailed",
    "cancelled",
    "canceled",
}
PROBLEM_STATUS_COMPACT = sorted({status.replace("_", "") for status in PROBLEM_STATUSES})


def normalize_status(status: Any) -> str:
    return str(status or "").strip().lower().replace("-", "_")


def compact_status(status: Any) -> str:
    return normalize_status(status).replace("_", "")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def connect_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def share_sync_report(root: Path) -> dict[str, Any]:
    db = root / "backend" / "config" / "share_sync" / "share_sync.db"
    conn = connect_ro(db)
    out: dict[str, Any] = {
        "db": str(db),
        "exists": conn is not None,
    }
    if conn is None:
        return out

    try:
        sub_cols = table_columns(conn, "share_subscriptions")
        optional_sub_cols = []
        for col in ("consecutive_link_failures", "link_invalid"):
            if col in sub_cols:
                optional_sub_cols.append(col)
            else:
                optional_sub_cols.append(f"NULL AS {col}")
        out["subscriptions"] = {
            "total": scalar(conn, "SELECT COUNT(*) FROM share_subscriptions") or 0,
            "enabled": scalar(conn, "SELECT COUNT(*) FROM share_subscriptions WHERE enabled = 1") or 0,
            "items": rows(
                conn,
                f"""
                SELECT id, name, enabled, owner_uid, {', '.join(optional_sub_cols)}
                FROM share_subscriptions
                ORDER BY updated_at DESC
                LIMIT 10
                """,
            ),
        }
        out["snapshots"] = {
            "total": scalar(conn, "SELECT COUNT(*) FROM share_snapshots") or 0,
            "items": scalar(conn, "SELECT COUNT(*) FROM share_snapshot_items") or 0,
        }
        out["runs"] = {
            "total": scalar(conn, "SELECT COUNT(*) FROM share_sync_runs") or 0,
            "recent": rows(
                conn,
                """
                SELECT id, subscription_id, status, total_count, added_count, modified_count,
                       failed_count, skipped_count, started_at, finished_at, COALESCE(error, '') AS error
                FROM share_sync_runs
                ORDER BY started_at DESC
                LIMIT 5
                """,
            ),
        }

        latest = conn.execute(
            "SELECT id FROM share_sync_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        if latest:
            run_id = latest["id"]
            out["latest_run"] = {
                "id": run_id,
                "item_status_counts": rows(
                    conn,
                    """
                    SELECT status, COUNT(*) AS count
                    FROM share_sync_run_items
                    WHERE run_id = ?
                    GROUP BY status
                    ORDER BY status
                    """,
                    (run_id,),
                ),
                "problem_items": rows(
                    conn,
                    f"""
                    SELECT id, path, status, COALESCE(error, '') AS error,
                           COALESCE(transfer_task_id, '') AS transfer_task_id,
                           COALESCE(download_task_id, '') AS download_task_id
                    FROM share_sync_run_items
                    WHERE run_id = ?
                      AND (
                          LOWER(REPLACE(status, '_', '')) IN ({",".join("?" for _ in PROBLEM_STATUS_COMPACT)})
                          OR COALESCE(error, '') <> ''
                      )
                    ORDER BY id
                    LIMIT 30
                    """,
                    (run_id, *PROBLEM_STATUS_COMPACT),
                ),
            }
        else:
            out["latest_run"] = None
    finally:
        conn.close()
    return out


def main_db_report(root: Path) -> dict[str, Any]:
    db = root / "backend" / "config" / "baidu-pcs.db"
    conn = connect_ro(db)
    out: dict[str, Any] = {
        "db": str(db),
        "exists": conn is not None,
    }
    if conn is None:
        return out

    try:
        table_names = [
            row["name"]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name
                """
            ).fetchall()
        ]
        interesting = [
            table
            for table in table_names
            if any(token in table for token in ("backup", "download", "task", "transfer"))
        ]
        status_tables = []
        problem_rows = []
        for table in interesting:
            cols = table_columns(conn, table)
            if "status" not in cols:
                continue
            counts = rows(
                conn,
                f"SELECT status, COUNT(*) AS count FROM {table} GROUP BY status ORDER BY status",
            )
            status_tables.append({"table": table, "counts": counts})

            error_col = "error" if "error" in cols else None
            id_col = next(
                (col for col in ("id", "task_id", "download_task_id", "transfer_task_id") if col in cols),
                None,
            )
            select_cols = ["status"]
            if id_col:
                select_cols.insert(0, id_col)
            if error_col:
                select_cols.append(f"COALESCE({error_col}, '') AS error")
            where = "LOWER(REPLACE(status, '_', '')) IN ({})".format(
                ",".join("?" for _ in PROBLEM_STATUS_COMPACT)
            )
            params: list[Any] = list(PROBLEM_STATUS_COMPACT)
            if error_col:
                where = f"({where} OR COALESCE({error_col}, '') <> '')"
            try:
                bad = rows(
                    conn,
                    f"SELECT {', '.join(select_cols)} FROM {table} WHERE {where} LIMIT 20",
                    tuple(params),
                )
            except sqlite3.OperationalError:
                bad = []
            if bad:
                problem_rows.append({"table": table, "rows": bad})

        out["status_tables"] = status_tables
        out["problem_rows"] = problem_rows
    finally:
        conn.close()
    return out


def subscription_baseline_counts(root: Path, subscription_id: str) -> dict[str, int]:
    db = root / "backend" / "config" / "share_sync" / "share_sync.db"
    conn = connect_ro(db)
    if conn is None:
        return {"snapshots": 0, "snapshot_items": 0, "runs": 0}
    try:
        snapshots = int(
            scalar(
                conn,
                "SELECT COUNT(*) FROM share_snapshots WHERE subscription_id = ?",
                (subscription_id,),
            )
            or 0
        )
        snapshot_items = int(
            scalar(
                conn,
                """
                SELECT COUNT(*)
                FROM share_snapshot_items
                WHERE snapshot_id IN (
                    SELECT id FROM share_snapshots WHERE subscription_id = ?
                )
                """,
                (subscription_id,),
            )
            or 0
        )
        runs = int(
            scalar(
                conn,
                "SELECT COUNT(*) FROM share_sync_runs WHERE subscription_id = ?",
                (subscription_id,),
            )
            or 0
        )
        return {"snapshots": snapshots, "snapshot_items": snapshot_items, "runs": runs}
    finally:
        conn.close()


def has_problems(report: dict[str, Any]) -> bool:
    share = report.get("share_sync", {})
    runs = share.get("runs", {})
    recent_runs = runs.get("recent", [])
    for run in recent_runs[:1]:
        status = normalize_status(run.get("status"))
        compact = compact_status(status)
        if status in PROBLEM_STATUSES or compact in PROBLEM_STATUSES:
            return True
        if int(run.get("failed_count") or 0) > 0:
            return True
    latest = share.get("latest_run") or {}
    if latest.get("problem_items"):
        return True
    main_db = report.get("main_db", {})
    return bool(main_db.get("problem_rows"))


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def run_metrics(run: dict[str, Any]) -> dict[str, Any]:
    started_at = _to_float(run.get("started_at"))
    finished_at = _to_float(run.get("finished_at"))
    duration_sec = None
    if started_at is not None and finished_at is not None and finished_at >= started_at:
        raw_delta = finished_at - started_at
        duration_sec = raw_delta / 1000.0 if raw_delta > 1_000_000 else raw_delta

    total = int(run.get("total_count") or 0)
    failed = int(run.get("failed_count") or 0)
    skipped = int(run.get("skipped_count") or 0)
    processed = total if total > 0 else None

    items_per_sec = None
    if duration_sec and duration_sec > 0 and processed is not None:
        items_per_sec = processed / duration_sec

    failure_rate = None
    if total > 0:
        failure_rate = failed / total

    return {
        "duration_sec": duration_sec,
        "items_per_sec": items_per_sec,
        "failure_rate": failure_rate,
        "failed": failed,
        "skipped": skipped,
        "total": total,
    }


def format_rate(value: Any) -> str:
    return "n/a" if value is None else f"{value:.4f}"


def format_percent(value: Any) -> str:
    return "n/a" if value is None else f"{value * 100:.4f}%"


def render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    share = report["share_sync"]
    lines.append(f"share_sync_db={share['db']} exists={share['exists']}")
    if share["exists"]:
        subs = share["subscriptions"]
        snaps = share["snapshots"]
        runs = share["runs"]
        lines.append(
            f"subscriptions total={subs['total']} enabled={subs['enabled']} "
            f"snapshots={snaps['total']} snapshot_items={snaps['items']} runs={runs['total']}"
        )
        for sub in subs["items"]:
            lines.append(
                "subscription "
                f"id={sub['id']} enabled={sub['enabled']} owner_uid={sub['owner_uid']} "
                f"name={sub['name']} link_invalid={sub['link_invalid']} "
                f"link_failures={sub['consecutive_link_failures']}"
            )
        for run in runs["recent"]:
            metrics = run_metrics(run)
            lines.append(
                "run "
                f"id={run['id']} status={run['status']} total={run['total_count']} "
                f"added={run['added_count']} modified={run['modified_count']} "
                f"failed={run['failed_count']} skipped={run['skipped_count']} "
                f"started_at={run['started_at']} finished_at={run['finished_at']} "
                f"duration_sec={format_rate(metrics['duration_sec'])} "
                f"items_per_sec={format_rate(metrics['items_per_sec'])} "
                f"failure_rate={format_percent(metrics['failure_rate'])} "
                f"error={run['error']}"
            )
        latest = share["latest_run"]
        if latest:
            lines.append(f"latest_run={latest['id']}")
            for row in latest["item_status_counts"]:
                lines.append(f"latest_item_status status={row['status']} count={row['count']}")
            for row in latest["problem_items"]:
                lines.append(
                    "problem_item "
                    f"id={row['id']} status={row['status']} path={row['path']} "
                    f"transfer={row['transfer_task_id']} download={row['download_task_id']} "
                    f"error={row['error']}"
                )
        else:
            lines.append("latest_run=None")

    main_db = report["main_db"]
    lines.append(f"main_db={main_db['db']} exists={main_db['exists']}")
    if main_db["exists"]:
        for table in main_db["status_tables"]:
            counts = ", ".join(f"{row['status']}={row['count']}" for row in table["counts"])
            lines.append(f"table_status {table['table']}: {counts}")
        for table in main_db["problem_rows"]:
            for row in table["rows"]:
                lines.append(f"problem_row {table['table']}: {row}")
    lines.append(f"problems={has_problems(report)}")
    return "\n".join(lines)


def build_report(root: Path) -> dict[str, Any]:
    return {
        "share_sync": share_sync_report(root),
        "main_db": main_db_report(root),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only share-sync/download status report")
    parser.add_argument("--root", type=Path, default=repo_root())
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--watch", type=float, default=0.0, help="Refresh interval seconds")
    parser.add_argument("--fail-on-problems", action="store_true")
    args = parser.parse_args()

    exit_code = 0
    while True:
        report = build_report(args.root.resolve())
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(render_text(report))
        if args.fail_on_problems and has_problems(report):
            exit_code = 2
        if args.watch <= 0:
            break
        print("---", flush=True)
        time.sleep(args.watch)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
