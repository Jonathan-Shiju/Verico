"""Load the presentation slice into Falkor window graphs.

Reads presentation/slice/accounts.csv and payments.csv. No Kafka. The original
transaction ids are kept. Refuses to write if a window already holds a large
graph, so this is not run against the full HI-Small Falkor by mistake.

Usage:
    .venv/bin/python scripts/load_presentation_slice.py --host 127.0.0.1
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from GraphIngest.Accounts import account_id, load_account_registry  # noqa: E402
from GraphIngest.GraphIngestor import INDEXES, QUERY_CHUNK, WRITE_ACCOUNTS, WRITE_TRANSFERS  # noqa: E402
from GraphIngest.Windows import WindowSpec, format_ts, parse_ts  # noqa: E402

SLICE = PROJECT_ROOT / "presentation" / "slice"
# The full graph has millions of edges. The slice has about eight thousand.
LARGE_GRAPH = 50_000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--payments", type=Path, default=SLICE / "payments.csv")
    parser.add_argument("--accounts", type=Path, default=SLICE / "accounts.csv")
    args = parser.parse_args()

    windows = WindowSpec(8 * 86400, 4 * 86400)
    graph_db = FalkorGraphDB(args.host, args.port)
    registry = load_account_registry(args.accounts)
    rows = _payments(args.payments)
    by_window: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        for start in windows.starts_for(row["ts"]):
            by_window[start].append(row)

    for start in by_window:
        _refuse_large(graph_db, windows.graph_name(start))

    latest = max(row["ts"] for row in rows)
    for start, window_rows in sorted(by_window.items()):
        graph = windows.graph_name(start)
        graph_db.ensure_indexes(graph, INDEXES)
        needed = {account for row in window_rows for account in (row["src"], row["dst"])}
        accounts = [registry[account] for account in sorted(needed)]
        missing = needed.difference(registry)
        if missing:
            raise RuntimeError(f"{graph} is missing {len(missing)} accounts from the registry")
        _chunks(graph_db, graph, WRITE_ACCOUNTS, "accounts", accounts)
        _chunks(graph_db, graph, WRITE_TRANSFERS, "rows", window_rows)
        graph_db.set_state(
            windows.windows_key,
            graph,
            json.dumps(
                {
                    "start": start,
                    "end": start + windows.length_seconds,
                    "status": "closed" if start + windows.length_seconds <= latest else "open",
                    "accounts_loaded": True,
                }
            ),
        )
        print(f"{graph} accounts={len(accounts)} payments={len(window_rows)}")

    graph_db.set_state(windows.watermark_key, "0", str(latest))
    graph_db.set_state(windows.watermark_key, "resolved", str(latest))
    print(f"payments={len(rows)} windows={len(by_window)} watermark={format_ts(latest)}")


def _payments(path: Path) -> list[dict]:
    found = []
    seen = set()
    with open(path, newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        for fields in reader:
            txn_id = int(fields[11])
            if txn_id in seen:
                continue
            seen.add(txn_id)
            found.append(
                {
                    "txn_id": txn_id,
                    "ts": parse_ts(fields[0]),
                    "src": account_id(fields[1], fields[2]),
                    "dst": account_id(fields[3], fields[4]),
                    "amount_paid": float(fields[7]),
                    "payment_currency": fields[8],
                    "amount_received": float(fields[5]),
                    "receiving_currency": fields[6],
                    "payment_format": fields[9],
                }
            )
    found.sort(key=lambda row: (row["ts"], row["txn_id"]))
    return found


def _refuse_large(graph_db: FalkorGraphDB, graph: str) -> None:
    try:
        rows = graph_db.query(graph, "MATCH ()-[t:TRANSFER]->() RETURN count(t)").result_set
    except Exception:
        return
    count = int(rows[0][0]) if rows else 0
    if count > LARGE_GRAPH:
        raise RuntimeError(f"{graph} already has {count} payments. Refusing to load the slice there.")


def _chunks(graph_db: FalkorGraphDB, graph: str, query: str, name: str, rows: list) -> None:
    for index in range(0, len(rows), QUERY_CHUNK):
        graph_db.query(graph, query, {name: rows[index : index + QUERY_CHUNK]})


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
