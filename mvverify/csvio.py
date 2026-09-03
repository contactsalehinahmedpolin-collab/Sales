"""CSV reading and writing for the lead list."""

from __future__ import annotations

import csv
from pathlib import Path


class ColumnNotFound(Exception):
    pass


def read_rows(path: Path, email_column: str) -> tuple[list[dict], list[str]]:
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        if email_column not in fieldnames:
            raise ColumnNotFound(
                f"column {email_column!r} not found in {path}; "
                f"columns are: {', '.join(fieldnames) or '(none)'}"
            )
        return list(reader), fieldnames


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
