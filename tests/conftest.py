import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeResponse:
    def __init__(self, payload=None, status_code=200, text=None):
        self._payload = payload
        self.status_code = status_code
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    """Realtime backend stub: one queued response (or list) per email."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, params=None, timeout=None):
        email = params["email"]
        self.calls.append(email)
        queued = self.responses[email]
        return queued.pop(0) if isinstance(queued, list) else queued


class FakeBulkSession:
    """Bulk backend stub: responses keyed by endpoint name.

    A list is consumed one entry per call and the last entry sticks, which models
    a job that reports progress before it finishes.
    """

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def request(self, method, url, params=None, files=None, timeout=None):
        path = url.rsplit("/", 1)[-1]
        self.calls.append((method, path, params, files))
        queued = self.routes[path]
        if isinstance(queued, list):
            return queued.pop(0) if len(queued) > 1 else queued[0]
        return queued

    def paths(self):
        return [call[1] for call in self.calls]


def ok_payload(email, result="ok", quality="good", error=""):
    return {
        "email": email,
        "result": result,
        "quality": quality,
        "error": error,
        "free": False,
        "role": False,
    }


def download_csv(rows, header=True):
    lines = ["email,quality,result,free,role"] if header else []
    lines += [f"{e},{q},{r},false,false" for e, q, r in rows]
    return "\n".join(lines) + "\n"


def write_leads(path, rows, header=("name", "email")):
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture
def responses():
    return FakeResponse


@pytest.fixture
def no_sleep():
    return lambda _seconds: None
