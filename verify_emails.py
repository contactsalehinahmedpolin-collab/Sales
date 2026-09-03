"""Verify a CSV of leads against the MillionVerifier API and split it by deliverability.

Every verification costs a credit, so this module is built around not spending them
twice: results are deduplicated, checkpointed to a local cache as they arrive, and
reused on the next run.

    export MILLIONVERIFIER_API_KEY=...
    python verify_emails.py leads.csv --out-dir out
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://api.millionverifier.com/api/v3/"
DEFAULT_CACHE = ".mv_cache.jsonl"

# MillionVerifier's documented `result` values, grouped by what you should do with them.
CLEAN_RESULTS = {"ok"}
RISKY_RESULTS = {"catch_all", "unknown"}
INVALID_RESULTS = {"invalid", "disposable"}

# Substrings of the API's `error` field that mean every further request will fail too.
FATAL_ERROR_MARKERS = ("api key", "credit", "suspended", "blocked", "not activated")

RESULT_COLUMNS = [
    "mv_result",
    "mv_quality",
    "mv_subresult",
    "mv_free",
    "mv_role",
    "mv_error",
    "mv_checked_at",
]


class FatalApiError(RuntimeError):
    """The account, not the email, is the problem — stop before burning the list."""


@dataclass
class Verification:
    email: str
    result: str = "error"
    quality: str = ""
    subresult: str = ""
    free: bool = False
    role: bool = False
    error: str = ""
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def bucket(self) -> str:
        if self.result in CLEAN_RESULTS:
            return "clean"
        if self.result in RISKY_RESULTS:
            return "risky"
        if self.result in INVALID_RESULTS:
            return "invalid"
        return "error"

    def as_row(self) -> dict[str, str]:
        return {
            "mv_result": self.result,
            "mv_quality": self.quality,
            "mv_subresult": self.subresult,
            "mv_free": str(self.free).lower(),
            "mv_role": str(self.role).lower(),
            "mv_error": self.error,
            "mv_checked_at": self.checked_at,
        }

    @classmethod
    def from_payload(cls, email: str, payload: dict) -> "Verification":
        return cls(
            email=email,
            result=str(payload.get("result") or "error"),
            quality=str(payload.get("quality") or ""),
            subresult=str(payload.get("subresult") or ""),
            free=bool(payload.get("free")),
            role=bool(payload.get("role")),
            error=str(payload.get("error") or ""),
        )


class RateLimiter:
    """Thread-safe pacing across workers. Unlike sleeping in the result loop, this
    actually delays the requests themselves."""

    def __init__(self, per_second: float):
        self._interval = 1.0 / per_second if per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self) -> None:
        if not self._interval:
            return
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_slot - now)
            self._next_slot = max(now, self._next_slot) + self._interval
        if wait:
            time.sleep(wait)


class ResultCache:
    """Append-only JSONL of verifications, keyed by email.

    Written as each result lands so an interrupted run keeps everything it paid for.
    """

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._entries: dict[str, Verification] = {}

    def load(self) -> int:
        if not self.path.exists():
            return 0
        with self.path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue  # tolerate a torn final line from a killed run
                email = record.get("email")
                if not email:
                    continue
                record.pop("bucket", None)
                try:
                    self._entries[email] = Verification(**record)
                except TypeError:
                    continue
        # A cached "error" is not a paid result; re-verify those.
        self._entries = {k: v for k, v in self._entries.items() if v.bucket != "error"}
        return len(self._entries)

    def get(self, email: str) -> Verification | None:
        return self._entries.get(email)

    def add(self, verification: Verification) -> None:
        with self._lock:
            self._entries[verification.email] = verification
            if verification.bucket == "error":
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(asdict(verification)) + "\n")


class Verifier:
    def __init__(
        self,
        api_key: str,
        rate_limiter: RateLimiter,
        session_factory=None,
        max_retries: int = 4,
        timeout: int = 20,
        sleep=time.sleep,
    ):
        self.api_key = api_key
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        self.timeout = timeout
        self._sleep = sleep
        self._session_factory = session_factory or (lambda: requests.Session())
        self._local = threading.local()
        self.abort = threading.Event()
        self.fatal_reason = ""

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._session_factory()
            self._local.session = session
        return session

    def _trip(self, reason: str) -> None:
        if not self.abort.is_set():
            self.fatal_reason = reason
            self.abort.set()

    def verify(self, email: str) -> Verification:
        last_error = ""
        for attempt in range(self.max_retries):
            if self.abort.is_set():
                return Verification(email=email, error=self.fatal_reason or "aborted")
            self.rate_limiter.acquire()
            try:
                response = self._session().get(
                    API_URL,
                    params={"api": self.api_key, "email": email, "timeout": 10},
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = f"request failed: {exc}"
                self._backoff(attempt)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"http {response.status_code}"
                self._backoff(attempt)
                continue
            if response.status_code >= 400:
                self._trip(f"http {response.status_code}: {response.text[:200]}")
                return Verification(email=email, error=self.fatal_reason)

            try:
                payload = response.json()
            except ValueError:
                last_error = f"non-json response: {response.text[:200]}"
                self._backoff(attempt)
                continue

            verification = Verification.from_payload(email, payload)
            if verification.error:
                lowered = verification.error.lower()
                if any(marker in lowered for marker in FATAL_ERROR_MARKERS):
                    self._trip(verification.error)
                    return verification
                last_error = verification.error
                self._backoff(attempt)
                continue
            if not payload.get("result"):
                last_error = f"no result in response: {str(payload)[:200]}"
                self._backoff(attempt)
                continue
            return verification

        return Verification(email=email, error=last_error or "exhausted retries")

    def _backoff(self, attempt: int) -> None:
        if attempt + 1 >= self.max_retries or self.abort.is_set():
            return
        self._sleep(min(2**attempt, 16) + random.random())


def normalize(email: str) -> str:
    return email.strip().lower()


def read_rows(path: Path, email_column: str) -> tuple[list[dict], list[str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        if email_column not in fieldnames:
            raise SystemExit(
                f"column {email_column!r} not found in {path}; columns are: {', '.join(fieldnames) or '(none)'}"
            )
        return list(reader), fieldnames


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run(args: argparse.Namespace) -> int:
    api_key = os.environ.get("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key and not args.dry_run:
        raise SystemExit("MILLIONVERIFIER_API_KEY is not set (see .env.example)")

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    rows, fieldnames = read_rows(input_path, args.email_column)

    cache = ResultCache(Path(args.cache))
    cached_count = cache.load()

    pending: list[str] = []
    seen: set[str] = set()
    for row in rows:
        email = normalize(row.get(args.email_column) or "")
        if not email or email in seen:
            continue
        seen.add(email)
        if cache.get(email) is None:
            pending.append(email)

    print(
        f"{len(rows)} rows | {len(seen)} unique emails | "
        f"{cached_count} cached | {len(pending)} to verify"
    )
    if args.dry_run:
        print(f"dry run: would spend ~{len(pending)} credits")
        return 0

    verifier = Verifier(api_key, RateLimiter(args.rate), max_retries=args.max_retries)
    if pending:
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for verification in pool.map(verifier.verify, pending):
                cache.add(verification)
                done += 1
                if done % 250 == 0 or done == len(pending):
                    print(f"  verified {done}/{len(pending)}", flush=True)

    out_fields = fieldnames + [c for c in RESULT_COLUMNS if c not in fieldnames]
    buckets: dict[str, list[dict]] = {"clean": [], "risky": [], "invalid": [], "error": []}
    annotated: list[dict] = []
    for row in rows:
        raw = row.get(args.email_column) or ""
        email = normalize(raw)
        if not email:
            verification = Verification(email="", error="no email in row")
        else:
            verification = cache.get(email) or Verification(
                email=email, error=verifier.fatal_reason or "not verified"
            )
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

    if verifier.abort.is_set():
        print(f"ABORTED: {verifier.fatal_reason}", file=sys.stderr)
        print("Fix the account issue and re-run; cached results are kept.", file=sys.stderr)
        return 2
    if buckets["error"]:
        print(f"{len(buckets['error'])} rows could not be verified; see {out_dir}/error.csv", file=sys.stderr)
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("input", help="input CSV of leads")
    parser.add_argument("--out-dir", default="out", help="directory for output CSVs (default: out)")
    parser.add_argument("--email-column", default="email", help="email column name (default: email)")
    parser.add_argument("--workers", type=int, default=20, help="concurrent requests (default: 20)")
    parser.add_argument(
        "--rate", type=float, default=50.0, help="max requests per second (default: 50, limit is 160)"
    )
    parser.add_argument("--max-retries", type=int, default=4, help="attempts per email (default: 4)")
    parser.add_argument("--cache", default=DEFAULT_CACHE, help=f"result cache path (default: {DEFAULT_CACHE})")
    parser.add_argument("--dry-run", action="store_true", help="report credit cost without verifying")
    return parser


def main(argv: list[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
