"""Review one stored lead and write keep or dismiss to aml:decisions.

The lead is a loop key (comma-separated transaction ids) or a residue id.
This does not read the transaction stream.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from GraphRead.reader import GraphReader  # noqa: E402
from Investigation.Decisions import DecisionStore  # noqa: E402
from Investigation.Investigator import review  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: investigate_lead.py <lead>")
    graph_db = FalkorGraphDB()
    result = review(GraphReader(graph_db), DecisionStore(graph_db), sys.argv[1])
    print(json.dumps(result, default=str))
    if result.get("error"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
