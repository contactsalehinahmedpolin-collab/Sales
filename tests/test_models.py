import pytest

from mvverify.models import Verification, as_bool, is_fatal_error, normalize


@pytest.mark.parametrize(
    "result,bucket",
    [
        ("ok", "clean"),
        ("catch_all", "risky"),
        ("unknown", "risky"),
        ("invalid", "invalid"),
        ("disposable", "invalid"),
    ],
)
def test_every_documented_result_lands_in_a_bucket(result, bucket):
    assert Verification(email="a@b.com", result=result).bucket == bucket


def test_unrecognized_result_is_an_error_not_a_silent_drop():
    assert Verification(email="a@b.com", result="something_new").bucket == "error"


def test_normalize_lowercases_and_strips():
    assert normalize("  A@B.com ") == "a@b.com"
    assert normalize(None) == ""


@pytest.mark.parametrize("message", ["Invalid API key.", "Insufficient credits", "Account suspended"])
def test_account_errors_are_fatal(message):
    assert is_fatal_error(message)


@pytest.mark.parametrize("message", ["", "timeout", "temporary failure"])
def test_transient_errors_are_not_fatal(message):
    assert not is_fatal_error(message)


@pytest.mark.parametrize("value,expected", [("1", True), ("true", True), ("YES", True), ("0", False), ("", False), (True, True)])
def test_as_bool_handles_the_shapes_the_api_uses(value, expected):
    assert as_bool(value) is expected


def test_as_row_exposes_every_result_column():
    from mvverify.models import RESULT_COLUMNS

    assert set(Verification(email="a@b.com", result="ok").as_row()) == set(RESULT_COLUMNS)
