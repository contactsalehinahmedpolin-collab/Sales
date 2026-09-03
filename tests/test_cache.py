import json

from mvverify.cache import ResultCache
from mvverify.models import Verification


def test_cache_round_trips_and_skips_errors(tmp_path):
    path = tmp_path / "cache.jsonl"
    cache = ResultCache(path)
    cache.add(Verification(email="a@b.com", result="ok"))
    cache.add(Verification(email="bad@b.com", result="error", error="timeout"))

    reloaded = ResultCache(path)
    assert reloaded.load() == 1
    assert reloaded.get("a@b.com").result == "ok"
    assert reloaded.get("bad@b.com") is None


def test_cache_tolerates_a_torn_line_from_a_killed_run(tmp_path):
    path = tmp_path / "cache.jsonl"
    path.write_text(
        json.dumps({"email": "a@b.com", "result": "ok"}) + '\n{"email": "b@c.co',
        encoding="utf-8",
    )
    assert ResultCache(path).load() == 1


def test_cache_ignores_records_with_unexpected_fields(tmp_path):
    path = tmp_path / "cache.jsonl"
    path.write_text(json.dumps({"email": "a@b.com", "result": "ok", "surprise": 1}) + "\n", encoding="utf-8")
    assert ResultCache(path).load() == 0


def test_missing_cache_file_is_not_an_error(tmp_path):
    assert ResultCache(tmp_path / "nope.jsonl").load() == 0
