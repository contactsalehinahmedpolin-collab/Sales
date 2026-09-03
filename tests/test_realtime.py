import pytest
from conftest import FakeResponse, FakeSession, ok_payload

from mvverify.realtime import RateLimiter, RealtimeVerifier


def make_verifier(responses, **kwargs):
    session = FakeSession(responses)
    verifier = RealtimeVerifier(
        "key",
        RateLimiter(0),
        session_factory=lambda: session,
        sleep=lambda _s: None,
        **kwargs,
    )
    return verifier, session


def test_parses_a_successful_verification():
    verifier, _ = make_verifier({"a@b.com": FakeResponse(ok_payload("a@b.com"))})
    v = verifier.verify("a@b.com")
    assert (v.result, v.quality, v.bucket) == ("ok", "good", "clean")


def test_retries_on_server_error_then_succeeds():
    verifier, session = make_verifier(
        {"a@b.com": [FakeResponse(status_code=503, text="boom"), FakeResponse(ok_payload("a@b.com"))]}
    )
    assert verifier.verify("a@b.com").result == "ok"
    assert len(session.calls) == 2


def test_retries_on_non_json_body_instead_of_crashing():
    verifier, session = make_verifier(
        {
            "a@b.com": [
                FakeResponse(payload=None, status_code=200, text="<html>502</html>"),
                FakeResponse(ok_payload("a@b.com")),
            ]
        }
    )
    assert verifier.verify("a@b.com").result == "ok"
    assert len(session.calls) == 2


def test_exhausted_retries_report_an_error_not_a_result():
    verifier, session = make_verifier({"a@b.com": [FakeResponse(status_code=500, text="x")] * 4}, max_retries=4)
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


def test_rate_limiter_paces_requests():
    import time

    limiter = RateLimiter(100)
    start = time.monotonic()
    for _ in range(5):
        limiter.acquire()
    assert time.monotonic() - start >= 0.03


def test_rate_limiter_of_zero_does_not_block():
    RateLimiter(0).acquire()
