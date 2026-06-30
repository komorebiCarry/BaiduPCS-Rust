#!/usr/bin/python3
"""Controlled share-sync trigger and monitor helper.

Default mode is read-only. Use --trigger SUBSCRIPTION_ID to start a sync run.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import share_sync_status


TERMINAL_STATUSES = {
    "completed",
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


def http_json(method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {url}: {raw}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"request failed {url}: {exc}") from exc


def list_subscriptions(base_url: str) -> list[dict[str, Any]]:
    resp = http_json("GET", f"{base_url}/api/v1/share-sync/subscriptions")
    data = resp.get("data") or []
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected subscriptions response: {resp}")
    return data


def trigger_subscription(base_url: str, subscription_id: str) -> str:
    resp = http_json(
        "POST",
        f"{base_url}/api/v1/share-sync/subscriptions/{subscription_id}/trigger",
    )
    if not resp.get("success", False):
        raise RuntimeError(f"trigger failed: {resp}")
    data = resp.get("data") or {}
    run_id = data.get("run_id")
    if not run_id:
        raise RuntimeError(f"trigger response missing run_id: {resp}")
    return str(run_id)


def fetch_run(base_url: str, run_id: str) -> dict[str, Any]:
    resp = http_json("GET", f"{base_url}/api/v1/share-sync/runs/{run_id}")
    if not resp.get("success", False):
        raise RuntimeError(f"run fetch failed: {resp}")
    data = resp.get("data") or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected run response: {resp}")
    return data


def api_run_has_problems(run: dict[str, Any]) -> bool:
    status = share_sync_status.normalize_status(run.get("status"))
    compact = share_sync_status.compact_status(status)
    if status in share_sync_status.PROBLEM_STATUSES:
        return True
    if compact in share_sync_status.PROBLEM_STATUSES:
        return True
    if int(run.get("failed_count") or 0) > 0:
        return True
    if run.get("error"):
        return True
    for item in run.get("items") or []:
        item_status = share_sync_status.normalize_status(item.get("status"))
        item_compact = share_sync_status.compact_status(item_status)
        if item_status in share_sync_status.PROBLEM_STATUSES:
            return True
        if item_compact in share_sync_status.PROBLEM_STATUSES:
            return True
        if item.get("error"):
            return True
    return False


def render_api_run(run: dict[str, Any]) -> str:
    metrics = share_sync_status.run_metrics(run)
    lines = [
        "api_run "
        f"id={run.get('id')} status={run.get('status')} total={run.get('total_count')} "
        f"added={run.get('added_count')} modified={run.get('modified_count')} "
        f"failed={run.get('failed_count')} skipped={run.get('skipped_count')} "
        f"duration_sec={share_sync_status.format_rate(metrics['duration_sec'])} "
        f"items_per_sec={share_sync_status.format_rate(metrics['items_per_sec'])} "
        f"failure_rate={share_sync_status.format_percent(metrics['failure_rate'])} "
        f"item_total_count={run.get('item_total_count')} error={run.get('error') or ''}"
    ]
    for item in run.get("items") or []:
        if item.get("error") or share_sync_status.normalize_status(item.get("status")) in share_sync_status.PROBLEM_STATUSES:
            lines.append(
                "api_problem_item "
                f"id={item.get('id')} status={item.get('status')} path={item.get('path')} "
                f"transfer={item.get('transfer_task_id')} download={item.get('download_task_id')} "
                f"error={item.get('error') or ''}"
            )
    return "\n".join(lines)


def latest_run(report: dict[str, Any]) -> dict[str, Any] | None:
    recent = report.get("share_sync", {}).get("runs", {}).get("recent", [])
    return recent[0] if recent else None


def run_status(report: dict[str, Any], run_id: str | None) -> str | None:
    if run_id:
        for run in report.get("share_sync", {}).get("runs", {}).get("recent", []):
            if run.get("id") == run_id:
                return share_sync_status.normalize_status(run.get("status"))
    run = latest_run(report)
    return share_sync_status.normalize_status(run.get("status")) if run else None


def print_subscriptions(subscriptions: list[dict[str, Any]]) -> None:
    for sub in subscriptions:
        targets = sub.get("targets") or []
        target_text = ",".join(
            str(t.get("local_path") or t.get("netdisk_path") or t.get("kind")) for t in targets
        )
        print(
            "subscription "
            f"id={sub.get('id')} enabled={sub.get('enabled')} owner_uid={sub.get('owner_uid')} "
            f"name={sub.get('name')} targets={target_text}"
        )


def monitor(base_url: str, root: Path, run_id: str | None, interval: float, timeout: float) -> int:
    started = time.monotonic()
    while True:
        if run_id:
            run = fetch_run(base_url, run_id)
            print(render_api_run(run), flush=True)
            status = share_sync_status.normalize_status(run.get("status"))
            status_compact = share_sync_status.compact_status(status)
            problems = api_run_has_problems(run)
            if problems:
                return 2
            if status in TERMINAL_STATUSES or status_compact in TERMINAL_STATUSES:
                return 0 if status == "completed" else 2
            if timeout > 0 and time.monotonic() - started >= timeout:
                print(f"timeout waiting for run_id={run_id} status={status}", file=sys.stderr)
                return 124
            print("---", flush=True)
            time.sleep(interval)
            continue

        report = share_sync_status.build_report(root)
        print(share_sync_status.render_text(report), flush=True)
        status = run_status(report, run_id)
        status_compact = share_sync_status.compact_status(status)
        problems = share_sync_status.has_problems(report)
        if problems:
            return 2
        if status in TERMINAL_STATUSES or status_compact in TERMINAL_STATUSES:
            return 0 if status == "completed" else 2
        if timeout > 0 and time.monotonic() - started >= timeout:
            print(f"timeout waiting for run_id={run_id or 'latest'} status={status}", file=sys.stderr)
            return 124
        print("---", flush=True)
        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="Controlled share-sync trigger and monitor helper")
    parser.add_argument("--root", type=Path, default=share_sync_status.repo_root())
    parser.add_argument("--base-url", default="http://127.0.0.1:4924")
    parser.add_argument("--list", action="store_true", help="List subscriptions via backend API")
    parser.add_argument("--trigger", metavar="SUBSCRIPTION_ID", help="Trigger one subscription")
    parser.add_argument(
        "--confirm-full-sync",
        action="store_true",
        help="Allow triggering when the local share-sync sqlite has no snapshots/runs",
    )
    parser.add_argument("--monitor", metavar="RUN_ID", help="Monitor an existing run id")
    parser.add_argument("--latest", action="store_true", help="Monitor latest run in sqlite")
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=0.0, help="Seconds, 0 means no timeout")
    args = parser.parse_args()

    root = args.root.resolve()

    if args.list:
        print_subscriptions(list_subscriptions(args.base_url))

    run_id = args.monitor
    if args.trigger:
        baseline = share_sync_status.subscription_baseline_counts(root, args.trigger)
        if (
            not args.confirm_full_sync
            and baseline["snapshots"] == 0
            and baseline["snapshot_items"] == 0
            and baseline["runs"] == 0
        ):
            print(
                "refusing to trigger: this subscription has snapshots=0 snapshot_items=0 runs=0; "
                "this may start a full sync/download. Re-run with --confirm-full-sync to proceed.",
                file=sys.stderr,
            )
            return 3
        run_id = trigger_subscription(args.base_url, args.trigger)
        print(f"triggered subscription={args.trigger} run_id={run_id}")

    if args.latest:
        run_id = None

    if args.trigger or args.monitor or args.latest:
        return monitor(args.base_url, root, run_id, args.interval, args.timeout)

    if not args.list:
        report = share_sync_status.build_report(root)
        print(share_sync_status.render_text(report))
        return 2 if share_sync_status.has_problems(report) else 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
