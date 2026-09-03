"""Command line entry point: read leads, verify what is not cached, split the list."""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .bulk import BulkApiError, BulkState, BulkVerifier
from .cache import ResultCache
from .csvio import ColumnNotFound, read_rows, write_csv
from .models import RESULT_COLUMNS, FatalApiError, Verification, normalize
from .realtime import RateLimiter, RealtimeVerifier

DEFAULT_CACHE = ".mv_cache.jsonl"
DEFAULT_BULK_STATE = ".mv_bulk.json"
BUCKETS = ("clean", "risky", "invalid", "error")


def resolve_mode(mode: str, pending: int, threshold: int) -> str:
    if mode != "auto":
        return mode
    return "bulk" if pending >= threshold else "single"


def collect_pending(rows: list[dict], email_column: str, cache: ResultCache) -> tuple[list[str], int]:
    """Unique, not-yet-verified emails — the only ones that will cost anything."""
    pending: list[str] = []
    seen: set[str] = set()
    for row in rows:
        email = normalize(row.get(email_column) or "")
        if not email or email in seen:
            continue
        seen.add(email)
        if cache.get(email) is None:
            pending.append(email)
    return pending, len(seen)


def run_single(args, api_key: str, pending: list[str], cache: ResultCache) -> str:
    verifier = RealtimeVerifier(
        api_key, RateLimiter(args.rate), max_retries=args.max_retries
    )
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for verification in pool.map(verifier.verify, pending):
            cache.add(verification)
            done += 1
            if done % 250 == 0 or done == len(pending):
                print(f"  verified {done}/{len(pending)}", flush=True)
    return verifier.fatal_reason if verifier.abort.is_set() else ""


def run_bulk(args, api_key: str, pending: list[str], cache: ResultCache) -> str:
    verifier = BulkVerifier(
        api_key,
        BulkState(Path(args.bulk_state)).load(),
        max_retries=args.max_retries,
        poll_interval=args.poll_interval,
        max_wait=args.bulk_timeout,
        delete_remote=args.delete_remote,
    )
    try:
        for verification in verifier.verify_many(pending, chunk_size=args.chunk_size):
            cache.add(verification)
    except (FatalApiError, BulkApiError) as exc:
        return str(exc)
    return ""


def run(args: argparse.Namespace) -> int:
    api_key = os.environ.get("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key and not args.dry_run:
        raise SystemExit("MILLIONVERIFIER_API_KEY is not set (see .env.example)")

    try:
        rows, fieldnames = read_rows(Path(args.input), args.email_column)
    except ColumnNotFound as exc:
        raise SystemExit(str(exc))

    cache = ResultCache(Path(args.cache))
    cached_count = cache.load()
    pending, unique = collect_pending(rows, args.email_column, cache)
    mode = resolve_mode(args.mode, len(pending), args.bulk_threshold)

    print(
        f"{len(rows)} rows | {unique} unique emails | {cached_count} cached | "
        f"{len(pending)} to verify | mode: {mode}"
    )
    if args.dry_run:
        print(f"dry run: would spend ~{len(pending)} credits")
        return 0

    failure = ""
    if pending:
        runner = run_bulk if mode == "bulk" else run_single
        failure = runner(args, api_key, pending, cache)

    out_dir = Path(args.out_dir)
    out_fields = fieldnames + [c for c in RESULT_COLUMNS if c not in fieldnames]
    buckets: dict[str, list[dict]] = {name: [] for name in BUCKETS}
    annotated: list[dict] = []
    unverified_reason = failure or "not verified"

    for row in rows:
        email = normalize(row.get(args.email_column) or "")
        if not email:
            verification = Verification(email="", error="no email in row")
        else:
            verification = cache.get(email) or Verification(email=email, error=unverified_reason)
        annotated_row = {**row, **verification.as_row()}
        annotated.append(annotated_row)
        buckets[verification.bucket].append(annotated_row)

    write_csv(out_dir / "all_results.csv", out_fields, annotated)
    for name, data in buckets.items():
        write_csv(out_dir / f"{name}.csv", out_fields, data)

    total = sum(len(v) for v in buckets.values())
    assert total == len(rows), f"row accounting mismatch: {total} != {len(rows)}"
    print(
        f"clean: {len(buckets['clean'])} | risky: {len(buckets['risky'])} | "
        f"invalid: {len(buckets['invalid'])} | error: {len(buckets['error'])} "
        f"(of {len(rows)} rows) -> {out_dir}/"
    )

    if failure:
        print(f"RUN FAILED: {failure}", file=sys.stderr)
        print("Cached results are kept; re-run to resume.", file=sys.stderr)
        return 2
    if buckets["error"]:
        print(
            f"{len(buckets['error'])} rows could not be verified; see {out_dir}/error.csv",
            file=sys.stderr,
        )
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify a CSV of leads against MillionVerifier and split it by deliverability."
    )
    parser.add_argument("input", help="input CSV of leads")
    parser.add_argument("--out-dir", default="out", help="directory for output CSVs (default: out)")
    parser.add_argument("--email-column", default="email", help="email column name (default: email)")
    parser.add_argument("--dry-run", action="store_true", help="report credit cost without verifying")
    parser.add_argument(
        "--mode",
        choices=("auto", "single", "bulk"),
        default="auto",
        help="verification backend (default: auto, by list size)",
    )
    parser.add_argument("--cache", default=DEFAULT_CACHE, help=f"result cache (default: {DEFAULT_CACHE})")
    parser.add_argument("--max-retries", type=int, default=4, help="attempts per request (default: 4)")

    single = parser.add_argument_group("single mode")
    single.add_argument("--workers", type=int, default=20, help="concurrent requests (default: 20)")
    single.add_argument(
        "--rate", type=float, default=50.0, help="requests per second ceiling (default: 50, limit is 160)"
    )

    bulk = parser.add_argument_group("bulk mode")
    bulk.add_argument(
        "--bulk-threshold",
        type=int,
        default=1000,
        help="in auto mode, use bulk at or above this many pending emails (default: 1000)",
    )
    bulk.add_argument("--chunk-size", type=int, default=50_000, help="emails per bulk file (default: 50000)")
    bulk.add_argument(
        "--bulk-state", default=DEFAULT_BULK_STATE, help=f"in-flight bulk file ids (default: {DEFAULT_BULK_STATE})"
    )
    bulk.add_argument("--poll-interval", type=float, default=15.0, help="seconds between polls (default: 15)")
    bulk.add_argument(
        "--bulk-timeout", type=float, default=3600.0, help="max seconds to wait per bulk file (default: 3600)"
    )
    bulk.add_argument(
        "--delete-remote", action="store_true", help="delete the uploaded file from MillionVerifier after download"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))
