"""Replay the stored payments and write unfinished chains into aml:loops.

A chain is at least two similar hops that never pay back into an account
already on the path. Closed loops already in the graph are left as they are.
"""

import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from adapters.staticdb import Redis  # noqa: E402
from Detection.LoopSink import FalkorLoopSink  # noqa: E402
from Detection.TreeLoopDetector import TreeConfig, TreeLoopDetector  # noqa: E402
from GraphIngest.GraphIngestor import to_row  # noqa: E402
from StreamReplay.TransactionReplay import TransactionStore  # noqa: E402

logger = logging.getLogger("write_open_chains")
WRITE_BATCH = 200


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    detector = TreeLoopDetector(TreeConfig())
    started = time.monotonic()
    seen = 0
    for _, transactions in TransactionStore(Redis()).iter_minutes("HI-Small_Trans"):
        for message in transactions:
            detector.process(to_row(message))
            seen += 1
        if seen and seen % 500_000 < len(transactions):
            logger.info(
                "processed=%s cycles=%s noted_open=%s open_leaves=%s",
                f"{seen:,}",
                f"{detector.stats['cycles']:,}",
                f"{detector.stats['open_chains']:,}",
                f"{detector.open_leaves():,}",
            )
    chains = detector.finish()
    logger.info("maximal open chains=%s", f"{len(chains):,}")
    sink = FalkorLoopSink(FalkorGraphDB())
    for start in range(0, len(chains), WRITE_BATCH):
        sink.write(chains[start : start + WRITE_BATCH])
    logger.info("Wrote %s open chains in %.0fs", f"{len(chains):,}", time.monotonic() - started)


if __name__ == "__main__":
    main()
