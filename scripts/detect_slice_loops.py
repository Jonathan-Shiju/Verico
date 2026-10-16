"""Detect closed loops in the presentation slice and write them to Falkor.

Reads payments.csv once, in time order. There is no Kafka and no tree graph.
The tree is the in-memory walk. Only closed loops are stored. Unfinished chains
are not written. Refuses to write when aml:loops already holds a large graph.

Usage:
    .venv/bin/python scripts/detect_slice_loops.py --payments presentation/slice/payments.csv
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from Detection.LoopSink import FalkorLoopSink  # noqa: E402
from Detection.TreeLoopDetector import TreeConfig, TreeLoopDetector  # noqa: E402
from GraphIngest.Windows import format_ts  # noqa: E402
from scripts.cli_utils import parse_duration  # noqa: E402
from scripts.load_presentation_slice import _payments  # noqa: E402

# The full graph holds thousands of loops. The slice should hold a handful.
LARGE_GRAPH = 100


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--payments", type=Path, default=PROJECT_ROOT / "presentation" / "slice" / "payments.csv")
    parser.add_argument("--loop-graph", default="aml:loops")
    parser.add_argument("--horizon", type=parse_duration, default=parse_duration("30d"))
    args = parser.parse_args()

    graph_db = FalkorGraphDB(args.host, args.port)
    _refuse_large(graph_db, args.loop_graph)
    detector = TreeLoopDetector(
        TreeConfig(
            horizon_seconds=int(args.horizon),
            max_depth=10,
            max_expansions_per_root=512,
            max_leaves_per_account=128,
            amount_tolerance=0.25,
            min_flow_fraction=0.05,
            spread_limit=100,
            excluded_self_formats=frozenset({"Reinvestment"}),
            self_skip_min_depth=1,
            self_skip_max_depth=None,
        )
    )
    sink = FalkorLoopSink(graph_db, args.loop_graph)
    rows = _payments(args.payments)
    written = 0
    for row in rows:
        cycles = detector.process(row)
        if cycles:
            sink.write(cycles)
            written += len(cycles)
    if rows:
        graph_db.set_state(f"{args.loop_graph}:watermark", "ts", str(rows[-1]["ts"]))
    print(f"payments={len(rows)} closed_loops={written} watermark={format_ts(rows[-1]['ts'])}")


def _refuse_large(graph_db: FalkorGraphDB, graph: str) -> None:
    try:
        rows = graph_db.query(graph, "MATCH (l:Loop) RETURN count(l)").result_set
    except Exception:
        return
    count = int(rows[0][0]) if rows else 0
    if count > LARGE_GRAPH:
        raise RuntimeError(f"{graph} already has {count} loops. Refusing to write the slice there.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
