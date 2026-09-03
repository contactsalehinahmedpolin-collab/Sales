"""Single-email backend: one HTTP request per address against the v3 API."""

from __future__ import annotations

import random
import threading
import time

import requests

from .models import Verification, is_fatal_error

API_URL = "https://api.millionverifier.com/api/v3/"


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


class RealtimeVerifier:
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
                if is_fatal_error(verification.error):
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
