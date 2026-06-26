#!/usr/bin/python3
"""Restore latest share-sync snapshots from a backup sqlite database.

Default mode is dry-run. Use --apply to write the current share_sync.db.
The script only imports latest snapshots for subscriptions that already exist
in the current database.
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def connect(path: Path, readonly: bool) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    else:
        conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def table_rows(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    return conn.execute(sql, params).fetchall()


def latest_snapshot(src: sqlite3.Connection, subscription_id: str) -> sqlite3.Row | None:
    return src.execute(
        """
        SELECT id, subscription_id, captured_at, item_count
        FROM share_snapshots
        WHERE subscription_id = ?
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (subscription_id,),
    ).fetchone()


def current_snapshot_count(dst: sqlite3.Connection, subscription_id: str) -> int:
    return int(
        dst.execute(
            "SELECT COUNT(*) FROM share_snapshots WHERE subscription_id = ?",
            (subscription_id,),
        ).fetchone()[0]
    )


def snapshot_item_count(conn: sqlite3.Connection, snapshot_id: str) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM share_snapshot_items WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()[0]
    )


def backup_current_db(dst_db: Path) -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup_path = dst_db.with_name(f"{dst_db.name}.pre-restore-{stamp}.bak")
    src_conn = connect(dst_db, readonly=True)
    try:
        backup_conn = sqlite3.connect(backup_path)
        try:
            src_conn.backup(backup_conn)
        finally:
            backup_conn.close()
    finally:
        src_conn.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(dst_db) + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, Path(str(backup_path) + suffix))
    return backup_path


def import_snapshot(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    snapshot: sqlite3.Row,
    replace: bool,
) -> tuple[int, int]:
    snapshot_id = snapshot["id"]
    subscription_id = snapshot["subscription_id"]

    if replace:
        existing = table_rows(
            dst,
            "SELECT id FROM share_snapshots WHERE subscription_id = ?",
            (subscription_id,),
        )
        for row in existing:
            dst.execute("DELETE FROM share_snapshot_items WHERE snapshot_id = ?", (row["id"],))
        dst.execute("DELETE FROM share_snapshots WHERE subscription_id = ?", (subscription_id,))

    dst.execute(
        """
        INSERT OR IGNORE INTO share_snapshots (id, subscription_id, captured_at, item_count)
        VALUES (?, ?, ?, ?)
        """,
        (
            snapshot["id"],
            snapshot["subscription_id"],
            snapshot["captured_at"],
            snapshot["item_count"],
        ),
    )
    inserted_snapshot = dst.execute("SELECT changes()").fetchone()[0]

    if inserted_snapshot:
        items = table_rows(
            src,
            """
            SELECT path, fs_id, size, is_dir, name
            FROM share_snapshot_items
            WHERE snapshot_id = ?
            ORDER BY id
            """,
            (snapshot_id,),
        )
        dst.executemany(
            """
            INSERT INTO share_snapshot_items (snapshot_id, path, fs_id, size, is_dir, name)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    snapshot_id,
                    item["path"],
                    item["fs_id"],
                    item["size"],
                    item["is_dir"],
                    item["name"],
                )
                for item in items
            ],
        )
        inserted_items = len(items)
    else:
        inserted_items = 0

    return int(inserted_snapshot), inserted_items


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore latest share-sync snapshots from backup")
    parser.add_argument("--root", type=Path, default=repo_root())
    parser.add_argument(
        "--source",
        type=Path,
        default=repo_root()
        / "backend"
        / "config"
        / "backups"
        / "share-sync-clean-20260616-163324"
        / "share_sync.db",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=repo_root() / "backend" / "config" / "share_sync" / "share_sync.db",
    )
    parser.add_argument("--subscription-id", action="append", default=[])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    source = args.source if args.source.is_absolute() else root / args.source
    target = args.target if args.target.is_absolute() else root / args.target

    if not source.exists():
        print(f"source db not found: {source}", file=sys.stderr)
        return 2
    if not target.exists():
        print(f"target db not found: {target}", file=sys.stderr)
        return 2

    src = connect(source, readonly=True)
    dst = connect(target, readonly=not args.apply)
    try:
        subscriptions = table_rows(
            dst,
            """
            SELECT id, name, owner_uid
            FROM share_subscriptions
            ORDER BY id
            """,
        )
        wanted = set(args.subscription_id)
        plan = []
        for sub in subscriptions:
            sub_id = sub["id"]
            if wanted and sub_id not in wanted:
                continue
            snap = latest_snapshot(src, sub_id)
            current_count = current_snapshot_count(dst, sub_id)
            if snap is None:
                plan.append((sub, None, current_count, 0))
                continue
            source_items = snapshot_item_count(src, snap["id"])
            plan.append((sub, snap, current_count, source_items))

        if not plan:
            print("no matching subscriptions found")
            return 1

        for sub, snap, current_count, source_items in plan:
            if snap is None:
                print(
                    "restore_plan "
                    f"subscription={sub['id']} name={sub['name']} current_snapshots={current_count} "
                    "source_snapshot=None action=skip"
                )
            else:
                action = "replace" if args.replace else ("skip_existing" if current_count else "import")
                print(
                    "restore_plan "
                    f"subscription={sub['id']} name={sub['name']} current_snapshots={current_count} "
                    f"source_snapshot={snap['id']} captured_at={snap['captured_at']} "
                    f"item_count={snap['item_count']} source_items={source_items} action={action}"
                )

        if not args.apply:
            print("dry_run=True; re-run with --apply to write target db")
            return 0

        backup_path = backup_current_db(target)
        print(f"target_backup={backup_path}")

        total_snapshots = 0
        total_items = 0
        with dst:
            for _sub, snap, current_count, _source_items in plan:
                if snap is None:
                    continue
                if current_count and not args.replace:
                    continue
                inserted_snapshot, inserted_items = import_snapshot(src, dst, snap, args.replace)
                total_snapshots += inserted_snapshot
                total_items += inserted_items

        print(f"restored snapshots={total_snapshots} items={total_items}")
        return 0
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    sys.exit(main())
