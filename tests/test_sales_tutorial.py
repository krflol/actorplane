"""Run the public tutorial as a user would, including replay and failed output."""
import csv
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

pytest.importorskip("pandas", reason="install the tutorial dependency group")

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "examples" / "sales_report.py"
SAMPLE = ROOT / "examples" / "data" / "sales.csv"
HEADER = ["order_id", "day", "customer", "amount_cents"]


def invoke(tmp_path, *, source=SAMPLE, report=None, database=None):
    return subprocess.run(
        [sys.executable, str(APP), "--input", str(source),
         "--db", str(database or tmp_path / "orders.sqlite3"),
         "--report", str(report or tmp_path / "revenue.csv")],
        capture_output=True, text=True, timeout=20,
    )


def ledger(tmp_path):
    with closing(sqlite3.connect(tmp_path / "orders.sqlite3")) as connection:
        return connection.execute(
            "SELECT order_id, day, customer, amount_cents FROM orders ORDER BY order_id"
        ).fetchall()


def write_input(path, rows):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(HEADER)
        writer.writerows(rows)
    return path


def test_import_replay_and_exact_report(tmp_path):
    first = invoke(tmp_path)
    assert first.returncode == 0, first.stdout + first.stderr
    rows = ledger(tmp_path)
    assert len(rows) == 5 and sum(row[3] for row in rows) == 24199
    report = tmp_path / "revenue.csv"
    original = report.read_bytes()
    with report.open(encoding="utf-8", newline="") as stream:
        totals = list(csv.DictReader(stream))
    assert [(row["day"], row["customer"], int(row["orders"]), int(row["total_cents"]))
            for row in totals] == [
        ("2026-09-20", "Acme", 2, 15000),
        ("2026-09-20", "Birch", 1, 4999),
        ("2026-09-21", "Acme", 1, 3000),
        ("2026-09-21", "Birch", 1, 1200),
    ]
    second = invoke(tmp_path)
    assert second.returncode == 0, second.stdout + second.stderr
    assert ledger(tmp_path) == rows
    assert report.read_bytes() == original


def test_conflicting_order_rolls_back_its_whole_batch(tmp_path):
    assert invoke(tmp_path).returncode == 0
    before = ledger(tmp_path)
    report = (tmp_path / "revenue.csv").read_bytes()
    source = write_input(tmp_path / "conflict.csv", [
        ("o9001", "2026-09-21", "Cedar", 100),
        ("o1001", "2026-09-20", "Acme", 999),
    ])
    result = invoke(tmp_path, source=source)
    assert result.returncode != 0
    assert ledger(tmp_path) == before
    assert (tmp_path / "revenue.csv").read_bytes() == report


def test_failed_report_can_be_rebuilt_from_committed_orders(tmp_path):
    blocked = tmp_path / "directory-not-a-csv"
    blocked.mkdir()
    result = invoke(tmp_path, report=blocked)
    assert result.returncode != 0
    assert len(ledger(tmp_path)) == 5
    assert blocked.is_dir()
    assert not list(tmp_path.glob("*.tmp"))
    recovered = invoke(tmp_path)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    assert len(ledger(tmp_path)) == 5
    assert (tmp_path / "revenue.csv").is_file()


def test_summary_limit_rejects_instead_of_silently_truncating(tmp_path):
    source = write_input(tmp_path / "many.csv", [
        (f"order-{i}", "2026-09-20", f"customer-{i}", 100)
        for i in range(257)
    ])
    report = tmp_path / "revenue.csv"
    report.write_text("previous report\n", encoding="utf-8")
    result = invoke(tmp_path, source=source)
    assert result.returncode != 0
    assert len(ledger(tmp_path)) == 257
    assert report.read_text(encoding="utf-8") == "previous report\n"


@pytest.mark.parametrize("collision", ["input", "database"])
def test_report_cannot_replace_input_or_database(tmp_path, collision):
    source = write_input(tmp_path / "orders.csv", [("one", "2026-09-20", "Acme", 100)])
    database = tmp_path / "orders.sqlite3"
    database.write_bytes(b"untouched existing database")
    before = source.read_bytes(), database.read_bytes()
    result = invoke(tmp_path, source=source, database=database,
                    report=source if collision == "input" else database)
    assert result.returncode != 0
    assert (source.read_bytes(), database.read_bytes()) == before


@pytest.mark.parametrize("row", [
    ("one", "20260920", "Acme", 100),
    ("one", "2026-02-30", "Acme", 100),
    ("one", "2026-09-20", "Acme", -1),
    ("one", "2026-09-20", "Acme", 100, "extra field"),
    ("one", "2026-09-20", "\u00e9" * 33, 100),
])
def test_invalid_first_batch_does_not_store_orders(tmp_path, row):
    source = write_input(tmp_path / "invalid.csv", [row])
    result = invoke(tmp_path, source=source)
    assert result.returncode != 0
    assert "CSV line 2" in result.stderr
    assert ledger(tmp_path) == []
    assert not (tmp_path / "revenue.csv").exists()


def test_empty_input_writes_a_header_only_report(tmp_path):
    result = invoke(tmp_path, source=write_input(tmp_path / "empty.csv", []))
    assert result.returncode == 0, result.stdout + result.stderr
    assert ledger(tmp_path) == []
    with (tmp_path / "revenue.csv").open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert "total_cents" in reader.fieldnames
        assert list(reader) == []
