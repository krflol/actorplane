"""Replay-safe CSV sales import, SQLite ledger, and pandas revenue report.

All database and report work happens in foreground actor callbacks.  Messages
carry only bounded, frozen schema values; the driver keeps one request live at
a time so retries and failures remain visible.
"""
from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated

from actorplane import Actor, FailurePolicy, HandlerFailed, IntRange, Length, World, event, handles


@event("sales.Sale")
class Sale:
    order_id: Annotated[str, Length(64)]
    day: Annotated[str, Length(10)]
    customer: Annotated[str, Length(64)]
    amount_cents: Annotated[int, IntRange(0, 100_000_000)]


@event("sales.StoreBatch")
class StoreBatch:
    sales: Annotated[tuple[Sale, ...], Length(32)]


@event("sales.Stored")
class Stored:
    inserted: int
    replayed: int


@event("sales.GetReport")
class GetReport:
    pass


@event("sales.SalesTotal")
class SalesTotal:
    day: Annotated[str, Length(10)]
    customer: Annotated[str, Length(64)]
    orders: int
    total_cents: int


@event("sales.ReportRows")
class ReportRows:
    rows: Annotated[tuple[SalesTotal, ...], Length(256)]


@event("sales.RenderReport")
class RenderReport:
    rows: ReportRows


@event("sales.ReportWritten")
class ReportWritten:
    rows: int
    orders: int
    total_cents: int


@dataclass(frozen=True)
class ImportStats:
    inserted: int
    replayed: int
    rows: int
    orders: int
    total_cents: int
    report_groups: int
    report_path: Path


class _Ledger(Actor):
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db: sqlite3.Connection | None = None

    def on_start(self, ctx):
        self.db = sqlite3.connect(self.db_path, timeout=1.0)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "order_id TEXT PRIMARY KEY NOT NULL, day TEXT NOT NULL, customer TEXT NOT NULL, "
            "amount_cents INTEGER NOT NULL CHECK(amount_cents BETWEEN 0 AND 100000000))"
        )
        self.db.commit()

    def on_stop(self, ctx):
        if self.db is not None:
            self.db.close()
            self.db = None

    @handles(StoreBatch)
    def store(self, event, ctx):
        assert self.db is not None
        inserted = replayed = 0
        # Reserve the write transaction before checking IDs, including replays.
        # The connection context commits on success and rolls back on error.
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            for sale in event.sales:
                existing = self.db.execute(
                    "SELECT day, customer, amount_cents FROM orders WHERE order_id=?",
                    (sale.order_id,),
                ).fetchone()
                if existing is None:
                    self.db.execute(
                        "INSERT INTO orders(order_id,day,customer,amount_cents) VALUES(?,?,?,?)",
                        (sale.order_id, sale.day, sale.customer, sale.amount_cents),
                    )
                    inserted += 1
                elif existing == (sale.day, sale.customer, sale.amount_cents):
                    replayed += 1
                else:
                    raise ValueError(f"conflicting sale for order_id {sale.order_id!r}")
        # Admission is not persistence: acknowledge only after the commit.
        ctx.reply(Stored(inserted, replayed))

    @handles(GetReport)
    def report(self, event, ctx):
        assert self.db is not None
        rows = self.db.execute(
            "SELECT day, customer, COUNT(*), SUM(amount_cents) "
            "FROM orders GROUP BY day, customer "
            "ORDER BY day, customer LIMIT 257"
        ).fetchall()
        if len(rows) > 256:
            raise ValueError("report has more than 256 day/customer groups")
        ctx.reply(ReportRows(tuple(SalesTotal(d, c, int(n), int(total)) for d, c, n, total in rows)))


class _Reporter(Actor):
    def __init__(self, report_path: Path):
        self.report_path = report_path

    @handles(RenderReport)
    def render(self, event, ctx):
        import pandas as pd

        rows = event.rows.rows
        records = [
            {"day": row.day, "customer": row.customer, "orders": row.orders,
             "total_cents": row.total_cents,
             "total": f"${row.total_cents // 100:,}.{row.total_cents % 100:02d}"}
            for row in rows
        ]
        frame = pd.DataFrame.from_records(
            records, columns=["day", "customer", "orders", "total_cents", "total"]
        )
        self.report_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", suffix=".tmp",
            dir=self.report_path.parent, delete=False,
        )
        temporary_name = Path(temporary.name)
        try:
            with temporary:
                frame.to_csv(temporary, index=False)
            os.replace(temporary_name, self.report_path)
        finally:
            temporary_name.unlink(missing_ok=True)
        ctx.reply(ReportWritten(len(rows), sum(row.orders for row in rows), sum(row.total_cents for row in rows)))


def _validate_paths(input_path: Path, db_path: Path, report_path: Path) -> None:
    paths = [path.resolve() for path in (input_path, db_path, report_path)]
    for index, left in enumerate(paths):
        for right in paths[index + 1:]:
            if left == right or (left.exists() and right.exists() and left.samefile(right)):
                raise ValueError("input, database, and report paths must be different")
    if paths[0].stat().st_size > 8 * 1024 * 1024:
        raise ValueError("input CSV exceeds the 8 MiB limit")


def _send(world, owner, target, event, timeout=5.0):
    operation = world.request(owner, target, event, timeout=timeout)
    while operation.poll() is None:
        # Drive callbacks outside handlers. The native request has a deadline;
        # it cannot preempt a running SQLite/pandas/file-system call.
        world.run_for(0.005)
    return operation.result()  # Consume the retained operation exactly once.


def _sale(record: dict) -> Sale:
    if None in record or any(value is None for value in record.values()):
        raise ValueError("each CSV row must have exactly four fields")
    for name in ("order_id", "customer"):
        if not record[name].strip() or len(record[name].encode("utf-8")) > 64:
            raise ValueError(f"{name} must be nonempty and at most 64 UTF-8 bytes")
    day = record["day"]
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("day must use YYYY-MM-DD")
    amount = record["amount_cents"]
    if not amount.isascii() or not amount.isdecimal():
        raise ValueError("amount_cents must be a nonnegative integer")
    cents = int(amount)
    if cents > 100_000_000:
        raise ValueError("amount_cents exceeds 100000000")
    return Sale(record["order_id"], day, record["customer"], cents)


def _ledger_actor(db_path: Path):
    class Ledger(_Ledger):
        def __init__(self):
            super().__init__(db_path)
    return Ledger


def _reporter_actor(report_path: Path):
    class Reporter(_Reporter):
        def __init__(self):
            super().__init__(report_path)
    return Reporter


def run_import(input_path: Path | str, db_path: Path | str, report_path: Path | str) -> ImportStats:
    input_path, db_path, report_path = map(Path, (input_path, db_path, report_path))
    _validate_paths(input_path, db_path, report_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    inserted = replayed = rows = 0
    with World(max_actors=4, mailbox_capacity=4, mailbox_bytes=32768,
               native_payload_budget=131072, max_operations=2, max_event_bytes=32768,
               failure_policy=FailurePolicy.STOP_WORLD) as world:
        owner = world.spawn(Actor)
        ledger = world.spawn(_ledger_actor(db_path), parent=owner)
        reporter = world.spawn(_reporter_actor(report_path), parent=owner)
        world.step()
        with input_path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            expected = ["order_id", "day", "customer", "amount_cents"]
            if reader.fieldnames != expected:
                raise ValueError(f"CSV headers must be exactly {expected}")
            batch = []
            for record in reader:
                try:
                    sale = _sale(record)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"CSV line {reader.line_num}: {exc}") from exc
                batch.append(sale)
                if len(batch) == 32:
                    result = _send(world, owner, ledger, StoreBatch(tuple(batch)))
                    inserted += result.inserted
                    replayed += result.replayed
                    rows += len(batch)
                    batch.clear()
            if batch:
                result = _send(world, owner, ledger, StoreBatch(tuple(batch)))
                inserted += result.inserted
                replayed += result.replayed
                rows += len(batch)
        summary = _send(world, owner, ledger, GetReport())
        written = _send(world, owner, reporter, RenderReport(summary))
        total = written.total_cents
        final_report = world.close(mode="drain")
        if (not final_report["native_done"] or not final_report["python_done"]
                or final_report["timed_out"]):
            raise RuntimeError("sales runtime did not shut down cleanly")
    return ImportStats(inserted, replayed, rows, written.orders, total, written.rows, report_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path(__file__).parent / "data" / "sales.csv")
    parser.add_argument("--db", type=Path, default=Path("target/sales-demo/orders.sqlite3"))
    parser.add_argument("--report", type=Path, default=Path("target/sales-demo/revenue.csv"))
    args = parser.parse_args()
    try:
        stats = run_import(args.input, args.db, args.report)
    except Exception as exc:
        if isinstance(exc, HandlerFailed) and exc.__cause__ is not None:
            exc = exc.__cause__
        print(f"sales import failed: {exc}", file=sys.stderr)
        return 1
    print(f"inserted={stats.inserted} replayed={stats.replayed} groups={stats.report_groups} orders={stats.orders} total_cents={stats.total_cents} report={stats.report_path} native_done=True python_done=True")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
