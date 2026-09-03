"""Bulk backend: upload the whole list as one file, poll, download the results.

One upload replaces N HTTP requests, which is the difference between minutes and
hours on a large list. The trade-off is that a bulk job is paid for at upload
time and runs server-side, so the file id is checkpointed to disk immediately:
if the process dies while polling, the next run re-attaches to the running job
instead of paying for it again.
"""

from __future__ import annotations

import csv
import io
import json
import random
import time
from hashlib import sha256
from pathlib import Path
from typing import Iterable, Iterator

import requests

from .models import KNOWN_RESULTS, FatalApiError, Verification, as_bool, is_fatal_error, normalize

BULK_API = "https://bulkapi.millionverifier.com/bulkapi/v2"
TERMINAL_FAILURES = {"canceled", "cancelled", "failed", "error", "unknown_error"}


class BulkApiError(RuntimeError):
    """The bulk job itself failed. Retrying the run is safe; the file id is kept."""


class BulkState:
    """Maps a chunk of emails to the bulk file id already paid for."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._entries: dict[str, dict] = {}

    def load(self) -> "BulkState":
        if self.path.exists():
            try:
                self._entries = json.loads(self.path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                self._entries = {}
        return self

    def get(self, key: str) -> str | None:
        entry = self._entries.get(key)
        return entry.get("file_id") if entry else None

    def set(self, key: str, file_id: str, count: int) -> None:
        self._entries[key] = {"file_id": file_id, "count": count, "created_at": time.time()}
        self._flush()

    def pop(self, key: str) -> None:
        if self._entries.pop(key, None) is not None:
            self._flush()

    def _flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._entries, indent=2), encoding="utf-8")
        tmp.replace(self.path)


def chunk_key(emails: Iterable[str]) -> str:
    digest = sha256("\n".join(sorted(emails)).encode("utf-8")).hexdigest()
    return digest[:32]


def chunked(items: list[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _cell(row: list[str], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return row[index].strip()


def _infer_columns(row: list[str]) -> dict[str, int]:
    """Locate columns in a header-less download by looking at the values."""
    index: dict[str, int] = {}
    for position, cell in enumerate(row):
        value = cell.strip()
        if "email" not in index and "@" in value:
            index["email"] = position
        if "result" not in index and value.lower() in KNOWN_RESULTS:
            index["result"] = position
    return index


def parse_download(text: str) -> dict[str, Verification]:
    """Turn a downloaded results CSV into verifications keyed by email.

    The column layout is resolved from the file rather than assumed, so a schema
    change on their side degrades into a clear error instead of silent mis-parsing.
    """
    rows = [row for row in csv.reader(io.StringIO(text)) if any(cell.strip() for cell in row)]
    if not rows:
        return {}

    header = [cell.strip().lower() for cell in rows[0]]
    if "email" in header or "result" in header:
        index = {name: position for position, name in enumerate(header)}
        body = rows[1:]
    else:
        index = _infer_columns(rows[0])
        body = rows

    if "email" not in index or "result" not in index:
        raise BulkApiError(
            f"could not locate email/result columns in the download; first row was {rows[0][:8]}"
        )

    parsed: dict[str, Verification] = {}
    for row in body:
        email = normalize(_cell(row, index.get("email")))
        if not email:
            continue
        result = _cell(row, index.get("result")).lower()
        recognized = result in KNOWN_RESULTS
        parsed[email] = Verification(
            email=email,
            result=result if recognized else "error",
            quality=_cell(row, index.get("quality")),
            subresult=_cell(row, index.get("subresult")),
            free=as_bool(_cell(row, index.get("free"))),
            role=as_bool(_cell(row, index.get("role"))),
            error="" if recognized else f"unrecognized result {result!r}",
        )
    return parsed


class BulkVerifier:
    def __init__(
        self,
        api_key: str,
        state: BulkState,
        session_factory=None,
        max_retries: int = 4,
        timeout: int = 120,
        poll_interval: float = 15.0,
        max_wait: float = 3600.0,
        delete_remote: bool = False,
        sleep=time.sleep,
        now=time.monotonic,
        log=print,
    ):
        self.api_key = api_key
        self.state = state
        self.max_retries = max_retries
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self.delete_remote = delete_remote
        self._sleep = sleep
        self._now = now
        self._log = log
        self._session = (session_factory or (lambda: requests.Session()))()

    def verify_many(self, emails: list[str], chunk_size: int = 50_000) -> Iterator[Verification]:
        chunks = list(chunked(emails, chunk_size))
        for number, chunk in enumerate(chunks, start=1):
            label = f"chunk {number}/{len(chunks)}"
            key = chunk_key(chunk)
            file_id = self.state.get(key)
            if file_id:
                self._log(f"  {label}: re-attaching to bulk file {file_id} (already paid for)")
            else:
                file_id = self._upload(chunk)
                self.state.set(key, file_id, len(chunk))
                self._log(f"  {label}: uploaded {len(chunk)} emails as bulk file {file_id}")

            self._wait(file_id, label)
            parsed = parse_download(self._download(file_id))
            self._log(f"  {label}: downloaded {len(parsed)} results")

            for email in chunk:
                yield parsed.get(email) or Verification(
                    email=email, error=f"missing from bulk file {file_id}"
                )

            self.state.pop(key)
            if self.delete_remote:
                self._delete(file_id)

    def _upload(self, emails: list[str]) -> str:
        payload = self._request(
            "POST",
            "upload",
            files={"file_contents": ("emails.txt", "\n".join(emails), "text/plain")},
        )
        file_id = payload.get("file_id")
        if not file_id:
            raise BulkApiError(f"upload returned no file_id: {str(payload)[:200]}")
        return str(file_id)

    def _wait(self, file_id: str, label: str) -> dict:
        deadline = self._now() + self.max_wait
        last_percent = None
        while True:
            info = self._request("GET", "fileinfo", params={"file_id": file_id})
            status = str(info.get("status") or "").lower()
            if status == "finished":
                return info
            if status in TERMINAL_FAILURES:
                raise BulkApiError(f"bulk file {file_id} ended with status {status!r}")

            percent = info.get("percent")
            if percent != last_percent:
                self._log(f"  {label}: {status or 'pending'} {percent if percent is not None else '?'}%")
                last_percent = percent
            if self._now() >= deadline:
                raise BulkApiError(
                    f"bulk file {file_id} still {status!r} after {self.max_wait:.0f}s; "
                    "re-run to resume, the job keeps running server-side"
                )
            self._sleep(self._poll_delay(info))

    def _poll_delay(self, info: dict) -> float:
        try:
            estimate = float(info.get("estimated_time_sec") or 0)
        except (TypeError, ValueError):
            estimate = 0.0
        if estimate <= 0:
            return self.poll_interval
        return max(5.0, min(estimate / 4, 60.0))

    def _download(self, file_id: str) -> str:
        text = self._request(
            "GET", "download", params={"file_id": file_id, "filter": "all"}, raw=True
        )
        stripped = text.lstrip()
        if stripped.startswith("{"):
            try:
                payload = json.loads(stripped)
            except ValueError:
                return text
            error = str(payload.get("error") or "")
            if error:
                if is_fatal_error(error):
                    raise FatalApiError(error)
                raise BulkApiError(error)
        return text

    def _delete(self, file_id: str) -> None:
        try:
            self._request("GET", "filedelete", params={"file_id": file_id})
        except (BulkApiError, FatalApiError) as exc:
            self._log(f"  could not delete remote file {file_id}: {exc}")

    def _request(self, method: str, path: str, *, params=None, files=None, raw: bool = False):
        query = {"key": self.api_key, **(params or {})}
        url = f"{BULK_API}/{path}"
        last_error = ""
        for attempt in range(self.max_retries):
            try:
                response = self._session.request(
                    method, url, params=query, files=files, timeout=self.timeout
                )
            except requests.RequestException as exc:
                last_error = f"request failed: {exc}"
                self._backoff(attempt)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"http {response.status_code}"
                self._backoff(attempt)
                continue
            if response.status_code in (401, 402, 403):
                raise FatalApiError(f"http {response.status_code}: {response.text[:200]}")
            if response.status_code >= 400:
                raise BulkApiError(f"http {response.status_code}: {response.text[:200]}")

            if raw:
                return response.text
            try:
                payload = response.json()
            except ValueError:
                last_error = f"non-json response: {response.text[:200]}"
                self._backoff(attempt)
                continue

            error = str(payload.get("error") or "")
            if error:
                if is_fatal_error(error):
                    raise FatalApiError(error)
                last_error = error
                self._backoff(attempt)
                continue
            return payload

        raise BulkApiError(f"{path}: {last_error or 'exhausted retries'}")

    def _backoff(self, attempt: int) -> None:
        if attempt + 1 >= self.max_retries:
            return
        self._sleep(min(2**attempt, 16) + random.random())
