"""Run the tier-1 window checks over transfers already stored in FalkorDB."""

import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from GraphIngest.WindowFlags import WindowFlagger  # noqa: E402
from GraphIngest.Windows import WindowSpec  # noqa: E402
from GraphRead.reader import GraphReader  # noqa: E402

logger = logging.getLogger("flag_windows")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    graph_db = FalkorGraphDB()
    flagger = WindowFlagger(graph_db)
    windows = [
        item["graph"]
        for item in GraphReader(graph_db, WindowSpec(8 * 86400, 4 * 86400)).list_windows()[
            "windows"
        ]
    ]
    for window in windows:
        started = time.monotonic()
        counts = flagger.scan_window(window)
        logger.info(
            "%s done: fan_in=%s fan_out=%s gather=%s in %.0fs",
            window,
            f"{counts['fan_in']:,}",
            f"{counts['fan_out']:,}",
            f"{counts['gather']:,}",
            time.monotonic() - started,
        )


if __name__ == "__main__":
    main()
