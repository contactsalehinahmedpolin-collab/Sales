import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import verify_emails as ve


class FakeResponse:
    def __init__(self, payload=None, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text if text else json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeSession:
    """Returns queued responses per email; records every call."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, params=None, timeout=None):
        email = params["email"]
        self.calls.append(email)
        queued = self.responses[email]
        return queued.pop(0) if isinstance(queued, list) else queued


def make_verifier(responses, **kwargs):
    session = FakeSession(responses)
    verifier = ve.Verifier(
        "key",
        ve.RateLimiter(0),
        session_factory=lambda: session,
        sleep=lambda _s: None,
        **kwargs,
    )
    return verifier, session


def ok_payload(email, result="ok", quality="good", error=""):
    return {"email": email, "result": result, "quality": quality, "error": error, "free": False, "role": False}


def test_parses_a_successful_verification():
    verifier, _ = make_verifier({"a@b.com": FakeResponse(ok_payload("a@b.com"))})
    v = verifier.verify("a@b.com")
    assert (v.result, v.quality, v.bucket) == ("ok", "good", "clean")


@pytest.mark.parametrize(
    "result,bucket",
    [("ok", "clean"), ("catch_all", "risky"), ("unknown", "risky"), ("invalid", "invalid"), ("disposable", "invalid")],
)
def test_every_documented_result_lands_in_a_bucket(result, bucket):
    assert ve.Verification(email="a@b.com", result=result).bucket == bucket


def test_unrecognized_result_is_an_error_not_a_silent_drop():
    assert ve.Verification(email="a@b.com", result="something_new").bucket == "error"


def test_retries_on_server_error_then_succeeds():
    verifier, session = make_verifier(
        {"a@b.com": [FakeResponse(status_code=503, text="boom"), FakeResponse(ok_payload("a@b.com"))]}
    )
    assert verifier.verify("a@b.com").result == "ok"
    assert len(session.calls) == 2


def test_retries_on_non_json_body_instead_of_crashing():
    verifier, session = make_verifier(
        {"a@b.com": [FakeResponse(payload=None, status_code=200, text="<html>502</html>"), FakeResponse(ok_payload("a@b.com"))]}
    )
    assert verifier.verify("a@b.com").result == "ok"
    assert len(session.calls) == 2


def test_exhausted_retries_report_an_error_not_a_result():
    verifier, session = make_verifier(
        {"a@b.com": [FakeResponse(status_code=500, text="x")] * 4}, max_retries=4
    )
    v = verifier.verify("a@b.com")
    assert v.bucket == "error" and "http 500" in v.error
    assert len(session.calls) == 4


def test_bad_api_key_trips_the_abort_switch():
    verifier, _ = make_verifier({"a@b.com": FakeResponse({"error": "Invalid API key.", "result": ""})})
    verifier.verify("a@b.com")
    assert verifier.abort.is_set()
    assert "api key" in verifier.fatal_reason.lower()


def test_abort_short_circuits_remaining_emails_without_spending_credits():
    verifier, session = make_verifier(
        {
            "a@b.com": FakeResponse({"error": "Insufficient credits.", "result": ""}),
            "c@d.com": FakeResponse(ok_payload("c@d.com")),
        }
    )
    verifier.verify("a@b.com")
    v = verifier.verify("c@d.com")
    assert session.calls == ["a@b.com"]
    assert v.bucket == "error"


def test_cache_round_trips_and_skips_errors(tmp_path):
    path = tmp_path / "cache.jsonl"
    cache = ve.ResultCache(path)
    cache.add(ve.Verification(email="a@b.com", result="ok"))
    cache.add(ve.Verification(email="bad@b.com", result="error", error="timeout"))

    reloaded = ve.ResultCache(path)
    assert reloaded.load() == 1
    assert reloaded.get("a@b.com").result == "ok"
    assert reloaded.get("bad@b.com") is None


def test_cache_tolerates_a_torn_line_from_a_killed_run(tmp_path):
    path = tmp_path / "cache.jsonl"
    path.write_text(json.dumps({"email": "a@b.com", "result": "ok"}) + '\n{"email": "b@c.co', encoding="utf-8")
    cache = ve.ResultCache(path)
    assert cache.load() == 1


def write_leads(path, rows, header=("name", "email")):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture
def cli(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")

    def _run(rows, responses, extra_args=()):
        leads = tmp_path / "leads.csv"
        write_leads(leads, rows)
        session = FakeSession(responses)
        monkeypatch.setattr(ve.requests, "Session", lambda: session)
        code = ve.main(
            [str(leads), "--out-dir", str(tmp_path / "out"), "--cache", str(tmp_path / "cache.jsonl"), "--rate", "0", *extra_args]
        )
        return code, tmp_path / "out", session

    return _run


def test_end_to_end_splits_rows_and_loses_none(cli):
    rows = [["A", "a@b.com"], ["B", "catch@b.com"], ["C", "bad@b.com"], ["D", ""]]
    responses = {
        "a@b.com": FakeResponse(ok_payload("a@b.com")),
        "catch@b.com": FakeResponse(ok_payload("catch@b.com", result="catch_all", quality="risky")),
        "bad@b.com": FakeResponse(ok_payload("bad@b.com", result="invalid", quality="bad")),
    }
    code, out, _ = cli(rows, responses)

    assert code == 1  # the row with no email is reported, not swallowed
    assert [r["email"] for r in read_csv(out / "clean.csv")] == ["a@b.com"]
    assert [r["email"] for r in read_csv(out / "risky.csv")] == ["catch@b.com"]
    assert [r["email"] for r in read_csv(out / "invalid.csv")] == ["bad@b.com"]
    assert read_csv(out / "error.csv")[0]["mv_error"] == "no email in row"
    assert len(read_csv(out / "all_results.csv")) == len(rows)


def test_duplicate_emails_are_verified_once_but_kept_in_output(cli):
    rows = [["A", "dup@b.com"], ["B", "DUP@b.com "], ["C", "dup@b.com"]]
    code, out, session = cli(rows, {"dup@b.com": FakeResponse(ok_payload("dup@b.com"))})

    assert code == 0
    assert session.calls == ["dup@b.com"]  # one credit, three rows
    assert len(read_csv(out / "clean.csv")) == 3


def test_second_run_reuses_the_cache_and_spends_nothing(cli):
    rows = [["A", "a@b.com"]]
    responses = {"a@b.com": FakeResponse(ok_payload("a@b.com"))}
    cli(rows, responses)
    _, out, session = cli(rows, responses)

    assert session.calls == []
    assert len(read_csv(out / "clean.csv")) == 1


def test_dry_run_spends_nothing(cli):
    code, _, session = cli([["A", "a@b.com"]], {}, extra_args=("--dry-run",))
    assert code == 0 and session.calls == []


def test_missing_email_column_fails_loudly(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")
    leads = tmp_path / "leads.csv"
    write_leads(leads, [["A", "a@b.com"]], header=("name", "work_email"))
    with pytest.raises(SystemExit):
        ve.main([str(leads), "--out-dir", str(tmp_path / "out"), "--cache", str(tmp_path / "c.jsonl")])


def test_empty_csv_produces_headers_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")
    leads = tmp_path / "leads.csv"
    write_leads(leads, [])
    out = tmp_path / "out"
    assert ve.main([str(leads), "--out-dir", str(out), "--cache", str(tmp_path / "c.jsonl")]) == 0
    assert read_csv(out / "clean.csv") == []


def test_rate_limiter_paces_requests():
    limiter = ve.RateLimiter(100)
    import time as _t

    start = _t.monotonic()
    for _ in range(5):
        limiter.acquire()
    assert _t.monotonic() - start >= 0.03
