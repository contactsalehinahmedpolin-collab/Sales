import pytest
from conftest import FakeResponse, FakeSession, ok_payload, read_csv, write_leads

import verify
from mvverify.models import Verification

HEADER = ("prospect_full_name", "prospect_company_name", "contact_mobile_phone", "prospect_linkedin", "contact_professional_email")


@pytest.mark.parametrize(
    "result,expected",
    [
        ("ok", "clean"),
        ("catch_all", "risky"),
        ("unknown", "risky"),
        ("invalid", "dead"),
        ("disposable", "dead"),
    ],
)
def test_three_way_split(result, expected):
    assert verify.bucket_for(Verification(email="a@b.com", result=result)) == expected


def test_a_failed_call_is_risky_never_dead():
    """An errored call says nothing about the mailbox; filing it dead discards leads."""
    assert verify.bucket_for(Verification(email="a@b.com", result="error", error="timeout")) == "risky"
    assert verify.bucket_for(Verification(email="", error="no email in row")) == "risky"


@pytest.fixture
def run_verify(tmp_path, monkeypatch):
    monkeypatch.setattr("mvverify.realtime.RealtimeVerifier._backoff", lambda self, attempt: None)

    def _run(rows, session, extra_args=()):
        leads = tmp_path / "leads.csv"
        write_leads(leads, rows, header=HEADER)
        monkeypatch.setattr("mvverify.realtime.requests.Session", lambda: session)
        code = verify.main(
            [
                str(leads),
                "--out-dir", str(tmp_path / "out"),
                "--cache", str(tmp_path / "cache.jsonl"),
                "--api-key", "test-key",
                "--rate", "0",
                *extra_args,
            ]
        )
        return code, tmp_path / "out"

    return _run


def lead(name, email):
    return [name, "Acme", "+15551234567", "linkedin.com/in/x", email]


def test_all_original_columns_survive(run_verify):
    session = FakeSession({"a@b.com": FakeResponse(ok_payload("a@b.com"))})
    _code, out = run_verify([lead("Ada", "a@b.com")], session)
    row = read_csv(out / "clean.csv")[0]
    assert row["prospect_full_name"] == "Ada"
    assert row["contact_mobile_phone"] == "+15551234567"
    assert row["prospect_linkedin"] == "linkedin.com/in/x"
    assert row["mv_result"] == "ok"


def test_rows_are_split_three_ways_and_none_are_lost(run_verify):
    rows = [lead("A", "a@b.com"), lead("B", "c@b.com"), lead("C", "d@b.com"), lead("D", "")]
    session = FakeSession(
        {
            "a@b.com": FakeResponse(ok_payload("a@b.com")),
            "c@b.com": FakeResponse(ok_payload("c@b.com", result="catch_all")),
            "d@b.com": FakeResponse(ok_payload("d@b.com", result="invalid")),
        }
    )
    code, out = run_verify(rows, session)

    assert code == 0
    assert len(read_csv(out / "clean.csv")) == 1
    assert len(read_csv(out / "risky.csv")) == 2  # catch_all + the row with no email
    assert len(read_csv(out / "dead.csv")) == 1


def test_an_api_failure_does_not_crash_the_run(run_verify):
    rows = [lead("A", "a@b.com"), lead("B", "boom@b.com")]
    session = FakeSession(
        {
            "a@b.com": FakeResponse(ok_payload("a@b.com")),
            "boom@b.com": [FakeResponse(status_code=500, text="server error")] * 4,
        }
    )
    code, out = run_verify(rows, session)

    assert code == 0
    assert len(read_csv(out / "clean.csv")) == 1
    assert len(read_csv(out / "risky.csv")) == 1


def test_console_output_never_leaks_emails_or_the_key(run_verify, capsys):
    session = FakeSession({"secret.person@example.com": FakeResponse(ok_payload("secret.person@example.com"))})
    run_verify([lead("A", "secret.person@example.com")], session)
    captured = capsys.readouterr()
    assert "secret.person@example.com" not in captured.out + captured.err
    assert "test-key" not in captured.out + captured.err


def test_placeholder_key_is_rejected(tmp_path, monkeypatch):
    monkeypatch.delenv("MILLIONVERIFIER_API_KEY", raising=False)
    leads = tmp_path / "leads.csv"
    write_leads(leads, [lead("A", "a@b.com")], header=HEADER)
    with pytest.raises(SystemExit, match="No MillionVerifier API key"):
        verify.main([str(leads), "--api-key", "[PASTE KEY HERE]"])


def test_finds_the_single_csv_in_a_folder(tmp_path, monkeypatch):
    write_leads(tmp_path / "export.csv", [lead("A", "a@b.com")], header=HEADER)
    assert verify.find_csv(tmp_path).name == "export.csv"


def test_output_files_are_not_mistaken_for_input(tmp_path):
    write_leads(tmp_path / "export.csv", [lead("A", "a@b.com")], header=HEADER)
    for name in ("clean.csv", "risky.csv", "dead.csv"):
        write_leads(tmp_path / name, [], header=HEADER)
    assert verify.find_csv(tmp_path).name == "export.csv"


def test_ambiguous_folder_asks_instead_of_guessing(tmp_path):
    write_leads(tmp_path / "a.csv", [], header=HEADER)
    write_leads(tmp_path / "b.csv", [], header=HEADER)
    with pytest.raises(SystemExit, match="several CSVs"):
        verify.find_csv(tmp_path)
