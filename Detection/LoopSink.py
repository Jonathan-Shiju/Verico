from dataclasses import asdict
import heapq
import itertools

from adapters.graphdb import GraphDB
from Detection.TreeLoopDetector import Cycle

LOOP_INDEXES = [
    "CREATE INDEX FOR (a:Account) ON (a.id)",
    "CREATE INDEX FOR (l:Loop) ON (l.key)",
]

WRITE_LOOPS = """
UNWIND $loops AS l
MERGE (c:Loop {key: l.key})
  ON CREATE SET c.txn_ids = l.txn_ids,
                c.length = size(l.txn_ids),
                c.start_ts = l.start_ts,
                c.end_ts = l.end_ts,
                c.amount_out = l.amount_out,
                c.amount_back = l.amount_back,
                c.tree_root = l.tree_root,
                c.closed_at_root = l.closed_at_root,
                c.open = l.open
WITH c, l
UNWIND range(0, size(l.accounts) - 1) AS i
MERGE (a:Account {id: l.accounts[i]})
MERGE (a)-[:IN_LOOP {position: i}]->(c)
"""

# Keeps each query's newly merged entities small (see GraphIngestor.QUERY_CHUNK).
LOOPS_PER_QUERY = 200


class FalkorLoopSink:
    def __init__(self, graph_db: GraphDB, graph: str = "aml:loops"):
        self.graph_db = graph_db
        self.graph = graph
        self.graph_db.ensure_indexes(graph, LOOP_INDEXES)

    def write(self, cycles: list[Cycle]) -> None:
        params = []
        for cycle in cycles:
            row = asdict(cycle)
            row["accounts"] = list(cycle.accounts)
            row["txn_ids"] = list(cycle.txn_ids)
            row["key"] = ",".join(map(str, cycle.txn_ids))
            row["open"] = bool(cycle.open)
            params.append(row)
        for i in range(0, len(params), LOOPS_PER_QUERY):
            self.graph_db.query(
                self.graph, WRITE_LOOPS, {"loops": params[i : i + LOOPS_PER_QUERY]}
            )


class EventTimeOrderer:
    """Merges time-ordered partitions into one time-ordered stream. An event is
    released once every partition has moved past its timestamp."""

    def __init__(self, partitions: int):
        self.partitions = partitions
        self.latest: dict[int, int] = {}
        self.heap: list = []
        self._seq = itertools.count()

    def add(self, partition: int, ts: int, item) -> None:
        self.latest[partition] = max(self.latest.get(partition, ts), ts)
        heapq.heappush(self.heap, (ts, next(self._seq), item))

    def low_watermark(self) -> int | None:
        if len(self.latest) < self.partitions:
            return None
        return min(self.latest.values())

    def release(self) -> list:
        low = self.low_watermark()
        out = []
        while self.heap and low is not None and self.heap[0][0] < low:
            out.append(heapq.heappop(self.heap)[2])
        return out

    def drain(self) -> list:
        out = [item for _, _, item in sorted(self.heap)]
        self.heap.clear()
        return out
