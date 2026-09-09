import pytest
from conftest import FakeBulkSession, FakeResponse, FakeSession, download_csv, ok_payload, read_csv, write_leads

from mvverify import cli


@pytest.fixture
def run_cli(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")

    def _run(rows, session, extra_args=()):
        leads = tmp_path / "leads.csv"
        write_leads(leads, rows)
        monkeypatch.setattr("mvverify.realtime.requests.Session", lambda: session)
        monkeypatch.setattr("mvverify.bulk.requests.Session", lambda: session)
        code = cli.main(
            [
                str(leads),
                "--out-dir", str(tmp_path / "out"),
                "--cache", str(tmp_path / "cache.jsonl"),
                "--bulk-state", str(tmp_path / "bulk.json"),
                "--rate", "0",
                *extra_args,
            ]
        )
        return code, tmp_path / "out"

    return _run


def realtime(responses):
    return FakeSession(responses)


def bulk(emails, results=None):
    results = results or [(e, "good", "ok") for e in emails]
    return FakeBulkSession(
        {
            "upload": FakeResponse({"file_id": "F1"}),
            "fileinfo": FakeResponse({"status": "finished", "percent": 100}),
            "download": FakeResponse(text=download_csv(results)),
        }
    )


# --- mode selection ---------------------------------------------------------


@pytest.mark.parametrize(
    "mode,pending,threshold,expected",
    [
        ("auto", 999, 1000, "single"),
        ("auto", 1000, 1000, "bulk"),
        ("single", 100000, 1000, "single"),
        ("bulk", 1, 1000, "bulk"),
    ],
)
def test_mode_resolution(mode, pending, threshold, expected):
    assert cli.resolve_mode(mode, pending, threshold) == expected


# --- shared behaviour, both backends ---------------------------------------


@pytest.mark.parametrize("mode", ["single", "bulk"])
def test_end_to_end_splits_rows_and_loses_none(run_cli, mode):
    rows = [["A", "a@b.com"], ["B", "catch@b.com"], ["C", "bad@b.com"], ["D", ""]]
    if mode == "single":
        session = realtime(
            {
                "a@b.com": FakeResponse(ok_payload("a@b.com")),
                "catch@b.com": FakeResponse(ok_payload("catch@b.com", result="catch_all", quality="risky")),
                "bad@b.com": FakeResponse(ok_payload("bad@b.com", result="invalid", quality="bad")),
            }
        )
    else:
        session = bulk(
            [],
            results=[("a@b.com", "good", "ok"), ("catch@b.com", "risky", "catch_all"), ("bad@b.com", "bad", "invalid")],
        )

    code, out = run_cli(rows, session, extra_args=("--mode", mode))

    assert code == 1  # the row with no email is reported, not swallowed
    assert [r["email"] for r in read_csv(out / "clean.csv")] == ["a@b.com"]
    assert [r["email"] for r in read_csv(out / "risky.csv")] == ["catch@b.com"]
    assert [r["email"] for r in read_csv(out / "invalid.csv")] == ["bad@b.com"]
    assert read_csv(out / "error.csv")[0]["mv_error"] == "no email in row"
    assert len(read_csv(out / "all_results.csv")) == len(rows)


@pytest.mark.parametrize("mode", ["single", "bulk"])
def test_duplicate_emails_are_verified_once_but_kept_in_output(run_cli, mode):
    rows = [["A", "dup@b.com"], ["B", "DUP@b.com "], ["C", "dup@b.com"]]
    session = realtime({"dup@b.com": FakeResponse(ok_payload("dup@b.com"))}) if mode == "single" else bulk(["dup@b.com"])

    code, out = run_cli(rows, session, extra_args=("--mode", mode))

    assert code == 0
    assert len(read_csv(out / "clean.csv")) == 3
    if mode == "single":
        assert session.calls == ["dup@b.com"]  # one credit, three rows
    else:
        _m, _p, _q, files = session.calls[0]
        assert files["file_contents"][1] == "dup@b.com"


@pytest.mark.parametrize("mode", ["single", "bulk"])
def test_second_run_reuses_the_cache_and_spends_nothing(run_cli, mode):
    rows = [["A", "a@b.com"]]
    first = realtime({"a@b.com": FakeResponse(ok_payload("a@b.com"))}) if mode == "single" else bulk(["a@b.com"])
    run_cli(rows, first, extra_args=("--mode", mode))

    second = FakeSession({}) if mode == "single" else FakeBulkSession({})
    code, out = run_cli(rows, second, extra_args=("--mode", mode))

    assert code == 0
    assert second.calls == []
    assert len(read_csv(out / "clean.csv")) == 1


@pytest.mark.parametrize("mode", ["single", "bulk"])
def test_account_failure_exits_two_and_keeps_partial_results(run_cli, mode):
    rows = [["A", "a@b.com"]]
    if mode == "single":
        session = realtime({"a@b.com": FakeResponse({"error": "Insufficient credits.", "result": ""})})
    else:
        session = FakeBulkSession({"upload": FakeResponse({"error": "Insufficient credits."})})

    code, out = run_cli(rows, session, extra_args=("--mode", mode))

    assert code == 2
    assert "Insufficient credits" in read_csv(out / "error.csv")[0]["mv_error"]


# --- input handling ---------------------------------------------------------


def test_dry_run_spends_nothing(run_cli):
    session = FakeSession({})
    code, _ = run_cli([["A", "a@b.com"]], session, extra_args=("--dry-run",))
    assert code == 0 and session.calls == []


def test_custom_email_column(run_cli, tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")
    leads = tmp_path / "custom.csv"
    write_leads(leads, [["A", "a@b.com"]], header=("name", "work_email"))
    session = realtime({"a@b.com": FakeResponse(ok_payload("a@b.com"))})
    monkeypatch.setattr("mvverify.realtime.requests.Session", lambda: session)
    code = cli.main(
        [
            str(leads),
            "--out-dir", str(tmp_path / "out2"),
            "--cache", str(tmp_path / "c.jsonl"),
            "--email-column", "work_email",
            "--rate", "0",
        ]
    )
    assert code == 0
    assert read_csv(tmp_path / "out2" / "clean.csv")[0]["work_email"] == "a@b.com"


def test_missing_email_column_fails_loudly(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")
    leads = tmp_path / "leads.csv"
    write_leads(leads, [["A", "a@b.com"]], header=("name", "work_email"))
    with pytest.raises(SystemExit, match="work_email"):
        cli.main([str(leads), "--out-dir", str(tmp_path / "out"), "--cache", str(tmp_path / "c.jsonl")])


def test_missing_api_key_fails_before_reading_anything(tmp_path, monkeypatch):
    monkeypatch.delenv("MILLIONVERIFIER_API_KEY", raising=False)
    with pytest.raises(SystemExit, match="MILLIONVERIFIER_API_KEY"):
        cli.main([str(tmp_path / "nope.csv")])


def test_empty_csv_produces_headers_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.setenv("MILLIONVERIFIER_API_KEY", "key")
    leads = tmp_path / "leads.csv"
    write_leads(leads, [])
    out = tmp_path / "out"
    assert cli.main([str(leads), "--out-dir", str(out), "--cache", str(tmp_path / "c.jsonl")]) == 0
    assert read_csv(out / "clean.csv") == []


def test_result_columns_are_appended_without_clobbering_input_columns(run_cli):
    session = realtime({"a@b.com": FakeResponse(ok_payload("a@b.com"))})
    _code, out = run_cli([["Ada", "a@b.com"]], session)
    row = read_csv(out / "all_results.csv")[0]
    assert row["name"] == "Ada" and row["mv_result"] == "ok" and row["mv_quality"] == "good"
