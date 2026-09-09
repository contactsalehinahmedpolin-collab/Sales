#!/usr/bin/env python3
"""Verify a Vibe Prospecting export against MillionVerifier and split it three ways.

    export MILLIONVERIFIER_API_KEY=...
    python verify.py                    # uses the single CSV in this folder
    python verify.py leads.csv          # or name one

Outputs clean.csv / risky.csv / dead.csv, each keeping every original column.
Nothing about an email address or the API key is ever printed.
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mvverify.cache import ResultCache
from mvverify.csvio import ColumnNotFound, read_rows, write_csv
from mvverify.models import RESULT_COLUMNS, Verification, normalize
from mvverify.realtime import RateLimiter, RealtimeVerifier

DEFAULT_EMAIL_COLUMN = "contact_professional_email"
DEFAULT_WORKERS = 10
DEFAULT_RATE = 40.0  # requests/sec, well under MillionVerifier's 160/sec ceiling

CLEAN = {"ok"}
RISKY = {"catch_all", "unknown"}


def bucket_for(verification: Verification) -> str:
    """clean / risky / dead, with anything that failed to verify treated as risky.

    A call that errored tells us nothing about the mailbox, so it must not be
    filed as dead — that would silently discard reachable leads.
    """
    if verification.result in CLEAN:
        return "clean"
    if verification.result in RISKY or verification.bucket == "error":
        return "risky"
    return "dead"


def find_csv(folder: Path) -> Path:
    candidates = sorted(
        path
        for path in folder.glob("*.csv")
        if path.name not in {"clean.csv", "risky.csv", "dead.csv"}
    )
    if not candidates:
        raise SystemExit(f"no input CSV found in {folder}")
    if len(candidates) > 1:
        names = ", ".join(p.name for p in candidates)
        raise SystemExit(f"several CSVs in {folder} ({names}); name the one to use")
    return candidates[0]


def run(args: argparse.Namespace) -> int:
    api_key = (args.api_key or os.environ.get("MILLIONVERIFIER_API_KEY", "")).strip()
    if api_key.startswith("[") or api_key.lower() in {"", "your_key_here", "paste key here"}:
        raise SystemExit(
            "No MillionVerifier API key. Set MILLIONVERIFIER_API_KEY or pass --api-key.\n"
            "The key is read from the environment on purpose: hardcoding it in this file "
            "would commit it to git."
        )

    source = Path(args.input) if args.input else find_csv(Path(args.folder))
    try:
        rows, fieldnames = read_rows(source, args.email_column)
    except ColumnNotFound as exc:
        raise SystemExit(str(exc))

    cache = ResultCache(Path(args.cache))
    cached = cache.load()

    # Unique addresses only — MillionVerifier bills per verification, and a
    # duplicate costs exactly as much as a new lead.
    pending: list[str] = []
    seen: set[str] = set()
    for row in rows:
        email = normalize(row.get(args.email_column) or "")
        if not email or email in seen:
            continue
        seen.add(email)
        if cache.get(email) is None:
            pending.append(email)

    print(f"{len(rows)} rows | {len(seen)} unique emails | {cached} cached | {len(pending)} to verify")
    if args.dry_run:
        print(f"dry run: would spend ~{len(pending)} credits")
        return 0

    verifier = RealtimeVerifier(api_key, RateLimiter(args.rate))
    if pending:
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for verification in pool.map(verifier.verify, pending):
                cache.add(verification)
                done += 1
                if done % 25 == 0 or done == len(pending):
                    print(f"  verified {done}/{len(pending)}", flush=True)

    out_dir = Path(args.out_dir)
    out_fields = fieldnames + [c for c in RESULT_COLUMNS if c not in fieldnames]
    buckets: dict[str, list[dict]] = {"clean": [], "risky": [], "dead": []}
    for row in rows:
        email = normalize(row.get(args.email_column) or "")
        if not email:
            verification = Verification(email="", error="no email in row")
        else:
            verification = cache.get(email) or Verification(
                email=email, error=verifier.fatal_reason or "not verified"
            )
        buckets[bucket_for(verification)].append({**row, **verification.as_row()})

    for name, data in buckets.items():
        write_csv(out_dir / f"{name}.csv", out_fields, data)

    total = sum(len(v) for v in buckets.values())
    assert total == len(rows), f"row accounting mismatch: {total} != {len(rows)}"
    print(
        f"\ntotal: {len(rows)} | clean: {len(buckets['clean'])} | "
        f"risky: {len(buckets['risky'])} | dead: {len(buckets['dead'])}"
    )
    print(f"written to {out_dir}/clean.csv, risky.csv, dead.csv")

    if verifier.abort.is_set():
        print(f"RUN FAILED: {verifier.fatal_reason}", file=sys.stderr)
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", nargs="?", help="input CSV (default: the one CSV in --folder)")
    parser.add_argument("--folder", default=".", help="folder to search for the input CSV (default: .)")
    parser.add_argument("--out-dir", default=".", help="where to write the three CSVs (default: .)")
    parser.add_argument("--email-column", default=DEFAULT_EMAIL_COLUMN, help=f"default: {DEFAULT_EMAIL_COLUMN}")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"default: {DEFAULT_WORKERS}")
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help=f"requests/sec, default: {DEFAULT_RATE}")
    parser.add_argument("--cache", default=".mv_cache.jsonl", help="result cache (default: .mv_cache.jsonl)")
    parser.add_argument("--api-key", default="", help="overrides MILLIONVERIFIER_API_KEY")
    parser.add_argument("--dry-run", action="store_true", help="report credit cost without verifying")
    return parser


def main(argv: list[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
