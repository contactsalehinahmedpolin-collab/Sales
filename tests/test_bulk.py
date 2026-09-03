import json

import pytest
from conftest import FakeBulkSession, FakeResponse, download_csv

from mvverify.bulk import BulkApiError, BulkState, BulkVerifier, chunk_key, parse_download
from mvverify.models import FatalApiError


def make_verifier(routes, tmp_path, **kwargs):
    session = FakeBulkSession(routes)
    verifier = BulkVerifier(
        "key",
        BulkState(tmp_path / "bulk.json").load(),
        session_factory=lambda: session,
        sleep=lambda _s: None,
        **kwargs,
    )
    verifier._log = lambda *_a, **_k: None
    return verifier, session


def finished(**extra):
    return FakeResponse({"file_id": "F1", "status": "finished", "percent": 100, **extra})


# --- download parsing -------------------------------------------------------


def test_parses_a_download_with_headers():
    parsed = parse_download(download_csv([("a@b.com", "good", "ok"), ("c@d.com", "bad", "invalid")]))
    assert parsed["a@b.com"].bucket == "clean"
    assert parsed["c@d.com"].bucket == "invalid"
    assert parsed["a@b.com"].quality == "good"


def test_infers_columns_when_the_download_has_no_header():
    parsed = parse_download(download_csv([("a@b.com", "good", "ok")], header=False))
    assert parsed["a@b.com"].result == "ok"


def test_emails_are_normalized_so_lookups_match():
    assert "a@b.com" in parse_download("email,result\n  A@B.COM ,ok\n")


def test_unrecognized_result_becomes_an_error_row_not_a_clean_one():
    parsed = parse_download("email,result\na@b.com,brand_new_status\n")
    assert parsed["a@b.com"].bucket == "error"
    assert "brand_new_status" in parsed["a@b.com"].error


def test_unlocatable_columns_raise_rather_than_mis_parse():
    with pytest.raises(BulkApiError, match="could not locate"):
        parse_download("alpha,beta\n1,2\n")


def test_empty_download_is_empty_not_an_error():
    assert parse_download("") == {}


def test_short_rows_do_not_raise():
    parsed = parse_download("email,quality,result\na@b.com,good\n")
    assert parsed["a@b.com"].bucket == "error"


# --- job lifecycle ----------------------------------------------------------


def test_full_cycle_uploads_polls_and_downloads(tmp_path):
    routes = {
        "upload": FakeResponse({"file_id": "F1", "status": "in_progress"}),
        "fileinfo": [FakeResponse({"status": "in_progress", "percent": 40}), finished()],
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok")])),
    }
    verifier, session = make_verifier(routes, tmp_path)
    results = list(verifier.verify_many(["a@b.com"]))

    assert [r.result for r in results] == ["ok"]
    assert session.paths() == ["upload", "fileinfo", "fileinfo", "download"]


def test_upload_body_is_the_newline_joined_email_list(tmp_path):
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok"), ("c@d.com", "good", "ok")])),
    }
    verifier, session = make_verifier(routes, tmp_path)
    list(verifier.verify_many(["a@b.com", "c@d.com"]))

    _method, _path, _params, files = session.calls[0]
    assert files["file_contents"][1] == "a@b.com\nc@d.com"


def test_emails_missing_from_the_download_are_reported_not_dropped(tmp_path):
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok")])),
    }
    verifier, _ = make_verifier(routes, tmp_path)
    results = {r.email: r for r in verifier.verify_many(["a@b.com", "ghost@b.com"])}

    assert results["ghost@b.com"].bucket == "error"
    assert "missing from bulk file" in results["ghost@b.com"].error


def test_large_lists_are_split_into_chunks(tmp_path):
    emails = [f"u{i}@b.com" for i in range(5)]
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([(e, "good", "ok") for e in emails])),
    }
    verifier, session = make_verifier(routes, tmp_path)
    results = list(verifier.verify_many(emails, chunk_size=2))

    assert len(results) == 5
    assert session.paths().count("upload") == 3


def test_a_running_job_is_resumed_instead_of_re_uploaded(tmp_path):
    emails = ["a@b.com"]
    state = BulkState(tmp_path / "bulk.json").load()
    state.set(chunk_key(emails), "F1", 1)

    routes = {
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok")])),
    }
    session = FakeBulkSession(routes)
    verifier = BulkVerifier(
        "key",
        BulkState(tmp_path / "bulk.json").load(),
        session_factory=lambda: session,
        sleep=lambda _s: None,
    )
    verifier._log = lambda *_a, **_k: None
    results = list(verifier.verify_many(emails))

    assert "upload" not in session.paths()  # the credits were already spent
    assert results[0].result == "ok"


def test_state_is_cleared_once_a_chunk_is_delivered(tmp_path):
    emails = ["a@b.com"]
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok")])),
    }
    verifier, _ = make_verifier(routes, tmp_path)
    list(verifier.verify_many(emails))

    assert BulkState(tmp_path / "bulk.json").load().get(chunk_key(emails)) is None


def test_file_id_is_saved_before_polling_starts(tmp_path):
    """If the process dies during the poll, the next run must not pay again."""
    emails = ["a@b.com"]
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": FakeResponse({"status": "in_progress", "percent": 10}),
        "download": FakeResponse(text=""),
    }
    verifier, _ = make_verifier(routes, tmp_path, max_wait=0)
    with pytest.raises(BulkApiError, match="still"):
        list(verifier.verify_many(emails))

    assert BulkState(tmp_path / "bulk.json").load().get(chunk_key(emails)) == "F1"


# --- failure handling -------------------------------------------------------


def test_canceled_job_raises(tmp_path):
    routes = {"upload": FakeResponse({"file_id": "F1"}), "fileinfo": FakeResponse({"status": "canceled"})}
    verifier, _ = make_verifier(routes, tmp_path)
    with pytest.raises(BulkApiError, match="canceled"):
        list(verifier.verify_many(["a@b.com"]))


def test_upload_without_a_file_id_raises(tmp_path):
    verifier, _ = make_verifier({"upload": FakeResponse({"status": "ok"})}, tmp_path)
    with pytest.raises(BulkApiError, match="no file_id"):
        list(verifier.verify_many(["a@b.com"]))


def test_account_error_in_json_is_fatal(tmp_path):
    verifier, _ = make_verifier({"upload": FakeResponse({"error": "Insufficient credits."})}, tmp_path)
    with pytest.raises(FatalApiError, match="credits"):
        list(verifier.verify_many(["a@b.com"]))


def test_payment_required_status_is_fatal(tmp_path):
    verifier, _ = make_verifier({"upload": FakeResponse(status_code=402, text="no credits")}, tmp_path)
    with pytest.raises(FatalApiError):
        list(verifier.verify_many(["a@b.com"]))


def test_json_error_returned_by_download_is_surfaced(tmp_path):
    routes = {
        "upload": FakeResponse({"file_id": "F1"}),
        "fileinfo": finished(),
        "download": FakeResponse(text=json.dumps({"error": "file not found"})),
    }
    verifier, _ = make_verifier(routes, tmp_path)
    with pytest.raises(BulkApiError, match="file not found"):
        list(verifier.verify_many(["a@b.com"]))


def test_transient_server_errors_are_retried(tmp_path):
    routes = {
        "upload": [FakeResponse(status_code=503, text="boom"), FakeResponse({"file_id": "F1"})],
        "fileinfo": finished(),
        "download": FakeResponse(text=download_csv([("a@b.com", "good", "ok")])),
    }
    verifier, session = make_verifier(routes, tmp_path)
    assert list(verifier.verify_many(["a@b.com"]))[0].result == "ok"
    assert session.paths().count("upload") == 2


def test_poll_delay_follows_the_servers_estimate(tmp_path):
    verifier, _ = make_verifier({}, tmp_path, poll_interval=15)
    assert verifier._poll_delay({}) == 15
    assert verifier._poll_delay({"estimated_time_sec": 120}) == 30
    assert verifier._poll_delay({"estimated_time_sec": 4}) == 5      # floor
    assert verifier._poll_delay({"estimated_time_sec": 9999}) == 60  # ceiling
    assert verifier._poll_delay({"estimated_time_sec": "junk"}) == 15


def test_state_file_survives_corruption(tmp_path):
    path = tmp_path / "bulk.json"
    path.write_text("{not json", encoding="utf-8")
    assert BulkState(path).load().get("anything") is None


def test_chunk_key_is_order_independent():
    assert chunk_key(["a@b.com", "c@d.com"]) == chunk_key(["c@d.com", "a@b.com"])
    assert chunk_key(["a@b.com"]) != chunk_key(["c@d.com"])
