"""Tier-1 flags read from a window graph after its transfers are written.

Fan-in and fan-out count distinct counterparties in that window. A gather
flag is an outgoing payment that matches the sum of the most recent incoming
payments in the same currency. Thresholds are settings, not dataset fits.
"""

from collections import defaultdict
from dataclasses import dataclass
import logging

from adapters.graphdb import GraphDB

logger = logging.getLogger(__name__)

FLAG_GRAPH = "aml:flags"

FLAG_INDEXES = [
    "CREATE INDEX FOR (f:Flag) ON (f.id)",
    "CREATE INDEX FOR (f:Flag) ON (f.account)",
    "CREATE INDEX FOR (f:Flag) ON (f.rule)",
]

# WITH is required. A WHERE id IN filter is dropped when the next clause
# expands a relationship into that account, so fan-in and gather saw no edges.
FAN_IN = """
MATCH (dst:Account) WHERE dst.id IN $ids
WITH dst
MATCH (src:Account)-[:TRANSFER]->(dst)
WHERE src.id <> dst.id
WITH dst, count(DISTINCT src) AS parties
WHERE parties >= $min_parties
RETURN dst.id AS account, parties
"""

FAN_OUT = """
MATCH (src:Account) WHERE src.id IN $ids
WITH src
MATCH (src)-[:TRANSFER]->(dst:Account)
WHERE src.id <> dst.id
WITH src, count(DISTINCT dst) AS parties
WHERE parties >= $min_parties
RETURN src.id AS account, parties
"""

# Bounded so a hub account is left to the fan-in / fan-out rules.
GATHER_CANDIDATES = """
MATCH (dst:Account) WHERE dst.id IN $ids
WITH dst
MATCH (src:Account)-[:TRANSFER]->(dst)
WHERE src.id <> dst.id
WITH dst, count(src) AS incoming
WHERE incoming >= $min_inflows AND incoming <= $max_edges
RETURN dst.id AS account
"""

EDGES = """
MATCH (dst:Account) WHERE dst.id IN $ids
WITH dst
MATCH (src:Account)-[t:TRANSFER]->(dst)
WHERE src.id <> dst.id
RETURN dst.id AS account, 'in' AS direction, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_received AS amount, t.receiving_currency AS currency
UNION
MATCH (src:Account) WHERE src.id IN $ids
WITH src
MATCH (src)-[t:TRANSFER]->(dst:Account)
WHERE src.id <> dst.id
RETURN src.id AS account, 'out' AS direction, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount, t.payment_currency AS currency
"""

WRITE_FLAGS = """
UNWIND $flags AS f
MERGE (n:Flag {id: f.id})
  ON CREATE SET n.rule = f.rule,
                n.account = f.account,
                n.window = f.window,
                n.parties = f.parties,
                n.txn_id = f.txn_id,
                n.amount = f.amount,
                n.inflow_sum = f.inflow_sum,
                n.inflow_txns = f.inflow_txns,
                n.ts = f.ts
"""

ACTIVE_ACCOUNTS = """
MATCH (a:Account)-[:TRANSFER]-()
RETURN DISTINCT a.id AS id
SKIP $skip
LIMIT $limit
"""

FAN_IN_WINDOW = """
MATCH (src:Account)-[:TRANSFER]->(dst:Account)
WHERE src.id <> dst.id
WITH dst, count(DISTINCT src) AS parties
WHERE parties >= $min_parties
RETURN dst.id AS account, parties
ORDER BY account
SKIP $skip
LIMIT $limit
"""

FAN_OUT_WINDOW = """
MATCH (src:Account)-[:TRANSFER]->(dst:Account)
WHERE src.id <> dst.id
WITH src, count(DISTINCT dst) AS parties
WHERE parties >= $min_parties
RETURN src.id AS account, parties
ORDER BY account
SKIP $skip
LIMIT $limit
"""

GATHER_WINDOW = """
MATCH (src:Account)-[:TRANSFER]->(dst:Account)
WHERE src.id <> dst.id
WITH dst, count(src) AS incoming
WHERE incoming >= $min_inflows AND incoming <= $max_edges
RETURN dst.id AS account
ORDER BY account
SKIP $skip
LIMIT $limit
"""


@dataclass
class WindowFlagConfig:
    # Distinct counterparties in one window before an account is flagged.
    min_parties: int = 8
    # A payout can clear between this many and gather_max_inflows recent inflows.
    gather_min_inflows: int = 2
    gather_max_inflows: int = 6
    # Accounts with more incoming payments than this are not sum-checked.
    gather_max_edges: int = 40
    # Payout may differ from the inflow sum by this fraction.
    amount_tolerance: float = 0.1
    account_chunk: int = 200


class WindowFlagger:
    def __init__(
        self,
        graph_db: GraphDB,
        config: WindowFlagConfig | None = None,
        flag_graph: str = FLAG_GRAPH,
    ):
        self.graph_db = graph_db
        self.config = config or WindowFlagConfig()
        self.flag_graph = flag_graph
        self._ready = False

    def check(self, window: str, account_ids: list[str]) -> int:
        """Flags the given accounts from the transfers already stored in `window`."""
        if not account_ids:
            return 0
        self._ensure()
        flags = []
        chunk = self.config.account_chunk
        for start in range(0, len(account_ids), chunk):
            ids = account_ids[start : start + chunk]
            flags.extend(self._fan(window, ids, "fan_in", FAN_IN))
            flags.extend(self._fan(window, ids, "fan_out", FAN_OUT))
            flags.extend(self._gather(window, ids))
        self._write(flags)
        return len(flags)

    def active_accounts(self, window: str, skip: int, limit: int) -> list[str]:
        result = self.graph_db.read(
            window, ACTIVE_ACCOUNTS, {"skip": skip, "limit": limit}
        )
        return [row[0] for row in result.result_set]

    def scan_window(self, window: str, page: int = 2_000) -> dict[str, int]:
        """Flags every account in one window. One aggregation per rule, paged."""
        self._ensure()
        counts = {"fan_in": 0, "fan_out": 0, "gather": 0}
        for rule, cypher in (("fan_in", FAN_IN_WINDOW), ("fan_out", FAN_OUT_WINDOW)):
            skip = 0
            while True:
                result = self.graph_db.read(
                    window,
                    cypher,
                    {
                        "min_parties": self.config.min_parties,
                        "skip": skip,
                        "limit": page,
                    },
                )
                rows = result.result_set
                if not rows:
                    break
                flags = [
                    self._fan_row(rule, window, row[0], int(row[1])) for row in rows
                ]
                self._write(flags)
                counts[rule] += len(flags)
                skip += len(rows)
                if len(rows) < page:
                    break
        cfg = self.config
        skip = 0
        while True:
            result = self.graph_db.read(
                window,
                GATHER_WINDOW,
                {
                    "min_inflows": cfg.gather_min_inflows,
                    "max_edges": cfg.gather_max_edges,
                    "skip": skip,
                    "limit": page,
                },
            )
            ids = [row[0] for row in result.result_set]
            if not ids:
                break
            chunk = cfg.account_chunk
            for start in range(0, len(ids), chunk):
                flags = self._gather(window, ids[start : start + chunk])
                self._write(flags)
                counts["gather"] += len(flags)
            skip += len(ids)
            if len(ids) < page:
                break
        return counts

    def _fan_row(self, rule: str, window: str, account: str, parties: int) -> dict:
        return {
            "id": f"{rule}:{window}:{account}",
            "rule": rule,
            "account": account,
            "window": window,
            "parties": parties,
            "txn_id": None,
            "amount": None,
            "inflow_sum": None,
            "inflow_txns": "",
            "ts": None,
        }

    def _fan(self, window: str, ids: list[str], rule: str, cypher: str) -> list[dict]:
        result = self.graph_db.read(
            window,
            cypher,
            {"ids": ids, "min_parties": self.config.min_parties},
        )
        return [
            self._fan_row(rule, window, row[0], int(row[1])) for row in result.result_set
        ]

    def _gather(self, window: str, ids: list[str]) -> list[dict]:
        cfg = self.config
        candidates = self.graph_db.read(
            window,
            GATHER_CANDIDATES,
            {
                "ids": ids,
                "min_inflows": cfg.gather_min_inflows,
                "max_edges": cfg.gather_max_edges,
            },
        )
        chosen = [row[0] for row in candidates.result_set]
        if not chosen:
            return []
        edges = self.graph_db.read(window, EDGES, {"ids": chosen}).result_set
        by_account: dict[str, list[tuple]] = defaultdict(list)
        for account, direction, txn_id, ts, amount, currency in edges:
            by_account[account].append((direction, int(txn_id), int(ts), float(amount), currency))

        flags = []
        for account, records in by_account.items():
            incoming = [r for r in records if r[0] == "in"]
            outgoing = [r for r in records if r[0] == "out"]
            for _, txn_id, ts, amount, currency in outgoing:
                if amount <= 0:
                    continue
                recent = sorted(
                    (r for r in incoming if r[2] <= ts and r[4] == currency),
                    key=lambda r: r[2],
                    reverse=True,
                )[: cfg.gather_max_inflows]
                matched = self._matching_prefix(amount, recent)
                if matched is None:
                    continue
                total, used = matched
                flags.append(
                    {
                        "id": f"gather:{window}:{txn_id}",
                        "rule": "gather",
                        "account": account,
                        "window": window,
                        "parties": len(used),
                        "txn_id": txn_id,
                        "amount": amount,
                        "inflow_sum": total,
                        "inflow_txns": ",".join(str(r[1]) for r in used),
                        "ts": ts,
                    }
                )
        return flags

    def _matching_prefix(self, amount: float, recent: list[tuple]) -> tuple[float, list] | None:
        cfg = self.config
        total = 0.0
        for index, record in enumerate(recent, start=1):
            total += record[3]
            if index < cfg.gather_min_inflows:
                continue
            if abs(amount - total) <= amount * cfg.amount_tolerance:
                return total, recent[:index]
        return None

    def _write(self, flags: list[dict]) -> None:
        for start in range(0, len(flags), 200):
            self.graph_db.query(
                self.flag_graph, WRITE_FLAGS, {"flags": flags[start : start + 200]}
            )

    def _ensure(self) -> None:
        if self._ready:
            return
        self.graph_db.ensure_indexes(self.flag_graph, FLAG_INDEXES)
        self._ready = True
