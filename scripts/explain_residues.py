"""Write pooled and split residue cases for transfers already stored in FalkorDB."""

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from Detection.ResidueCases import ResidueScanner  # noqa: E402
from GraphIngest.Windows import WindowSpec  # noqa: E402
from GraphRead.reader import GraphReader  # noqa: E402

logger = logging.getLogger("explain_residues")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window", action="append", default=[], help="Limit to one window graph. Repeatable.")
    parser.add_argument(
        "--one-each",
        action="store_true",
        help="Stop after one pooled case and one split case. For a small instance.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    graph_db = FalkorGraphDB()
    scanner = ResidueScanner(graph_db)
    windows = args.window or [
        item["graph"]
        for item in GraphReader(graph_db, WindowSpec(8 * 86400, 4 * 86400)).list_windows()["windows"]
    ]
    need = {"pooled": 1, "split": 1} if args.one_each else None
    for window in windows:
        started = time.monotonic()
        counts = scanner.scan_window(window, need)
        logger.info(
            "%s done: accounts=%s pooled=%s split=%s in %.0fs",
            window,
            f"{counts['accounts']:,}",
            f"{counts['pooled']:,}",
            f"{counts['split']:,}",
            time.monotonic() - started,
        )
        if need is not None and all(left <= 0 for left in need.values()):
            break


if __name__ == "__main__":
    main()
