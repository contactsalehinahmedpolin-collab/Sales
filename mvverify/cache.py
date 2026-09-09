"""Append-only result cache — the reason an interrupted run does not cost twice."""

from __future__ import annotations

import json
import threading
from dataclasses import asdict
from pathlib import Path

from .models import Verification


class ResultCache:
    """JSONL of verifications keyed by email, written as each result lands.

    Only paid results are persisted; `error` records are dropped so a failure
    retries on the next run instead of sticking forever.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
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
                if not record.get("email"):
                    continue
                record.pop("bucket", None)
                try:
                    verification = Verification(**record)
                except TypeError:
                    continue
                if verification.bucket != "error":
                    self._entries[verification.email] = verification
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
