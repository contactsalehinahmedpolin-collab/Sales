"""Result vocabulary and the record every backend produces."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

# MillionVerifier's documented `result` values, grouped by what you should do with them.
CLEAN_RESULTS = {"ok"}
RISKY_RESULTS = {"catch_all", "unknown"}
INVALID_RESULTS = {"invalid", "disposable"}
KNOWN_RESULTS = CLEAN_RESULTS | RISKY_RESULTS | INVALID_RESULTS

# Substrings of the API's `error` field that mean every further request will fail too.
FATAL_ERROR_MARKERS = ("api key", "credit", "suspended", "blocked", "not activated", "forbidden")

RESULT_COLUMNS = [
    "mv_result",
    "mv_quality",
    "mv_subresult",
    "mv_free",
    "mv_role",
    "mv_error",
    "mv_checked_at",
]

TRUTHY = {"1", "true", "yes", "y", "t"}


class FatalApiError(RuntimeError):
    """The account, not the email, is the problem — stop before burning the list."""


def is_fatal_error(message: str) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in FATAL_ERROR_MARKERS)


def normalize(email: str) -> str:
    return (email or "").strip().lower()


def as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in TRUTHY


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
            free=as_bool(payload.get("free")),
            role=as_bool(payload.get("role")),
            error=str(payload.get("error") or ""),
        )
