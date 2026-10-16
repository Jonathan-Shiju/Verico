"""Cases left after one-to-one amount matching.

A similar incoming payment and a later outgoing payment are what the tree
already walks, so they are paired and set aside. What remains is either a set
of incoming payments that together match one outgoing payment, or one incoming
payment that together matches a set of outgoing payments. Hubs are left out:
an account with a wide set of counterparties always has a residue, and that
residue is ordinary activity.
"""

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
import logging

from adapters.graphdb import GraphDB

logger = logging.getLogger(__name__)

CASE_GRAPH = "aml:cases"

CASE_INDEXES = [
    "CREATE INDEX FOR (c:Case) ON (c.id)",
    "CREATE INDEX FOR (c:Case) ON (c.account)",
]

ENTITY_INDEX = "CREATE INDEX FOR (a:Account) ON (a.entity_id)"

# Carry the account forward with WITH. A WHERE id IN filter is dropped when
# the next clause expands a relationship into that account.
ELIGIBLE = """
MATCH (a:Account) WHERE a.id IN $ids
WITH a
MATCH (a)-[:TRANSFER]->(b:Account)
WHERE a.id <> b.id
WITH a, count(DISTINCT b) AS out_parties
WHERE out_parties > 0 AND out_parties <= $spread
WITH a
MATCH (c:Account)-[:TRANSFER]->(a)
WHERE c.id <> a.id
WITH a, count(DISTINCT c) AS in_parties
WHERE in_parties > 0 AND in_parties <= $spread
RETURN a.id AS id
"""

EDGES = """
MATCH (a:Account) WHERE a.id IN $ids
WITH a
MATCH (a)-[t:TRANSFER]->(b:Account)
WHERE a.id <> b.id
RETURN a.id AS account, 'out' AS direction, b.id AS other, t.txn_id AS txn_id,
       t.ts AS ts, t.amount_paid AS amount, t.payment_currency AS currency
UNION
MATCH (a:Account) WHERE a.id IN $ids
WITH a
MATCH (b:Account)-[t:TRANSFER]->(a)
WHERE a.id <> b.id
RETURN a.id AS account, 'in' AS direction, b.id AS other, t.txn_id AS txn_id,
       t.ts AS ts, t.amount_received AS amount, t.receiving_currency AS currency
"""

WRITE_CASES = """
UNWIND $cases AS c
MERGE (n:Case {id: c.id})
  ON CREATE SET n.kind = c.kind,
                n.account = c.account,
                n.window = c.window,
                n.focal_txn = c.focal_txn,
                n.part_txns = c.part_txns,
                n.amount = c.amount,
                n.part_sum = c.part_sum,
                n.currency = c.currency,
                n.ts = c.ts
"""

ACTIVE_ACCOUNTS = """
MATCH (a:Account)-[:TRANSFER]-()
RETURN DISTINCT a.id AS id
SKIP $skip
LIMIT $limit
"""


@dataclass
class ResidueConfig:
    # Same slack the tree uses when one payment continues another.
    amount_tolerance: float = 0.25
    # A same-currency set has to land this close to count as the explanation.
    # The wider tree slack is for a single hop, where conversion can move the amount.
    sum_tolerance: float = 0.1
    # Accounts with more distinct counterparties than this are not examined.
    spread_limit: int = 100
    # A residue explains a payment with between 2 and this many other payments.
    max_parts: int = 6
    # Only the payments closest in time to the focal payment are combined.
    max_candidates: int = 12
    # Compute bound: cases kept for one account, largest focal amount first.
    max_cases_per_account: int = 4
    account_chunk: int = 50
    page: int = 500


@dataclass(frozen=True)
class Payment:
    txn_id: int
    ts: int
    amount: float
    currency: str
    other: str
    direction: str


def similar(amount: float, target: float, tolerance: float) -> bool:
    if target <= 0 or amount <= 0:
        return False
    return abs(amount - target) <= target * tolerance


def explain_account(payments: list[Payment], config: ResidueConfig | None = None) -> list[dict]:
    """Pooled and split residues for one account. Self-transfers are already gone."""
    cfg = config or ResidueConfig()
    by_currency: dict[str, list[Payment]] = defaultdict(list)
    for payment in payments:
        if payment.amount <= 0:
            continue
        by_currency[payment.currency].append(payment)

    cases = []
    for currency, rows in by_currency.items():
        incoming = [row for row in rows if row.direction == "in"]
        outgoing = [row for row in rows if row.direction == "out"]
        used = _pair_similar(incoming, outgoing, cfg.amount_tolerance)
        cases.extend(_residues(incoming, outgoing, used, currency, cfg))
    cases.sort(key=lambda case: case["amount"], reverse=True)
    return cases[: cfg.max_cases_per_account]


def _pair_similar(incoming: list[Payment], outgoing: list[Payment], tolerance: float) -> set[int]:
    """Pair each outgoing payment with the latest earlier incoming payment of a similar amount."""
    used: set[int] = set()
    incoming_by_time = sorted(incoming, key=lambda payment: payment.ts)
    for payment in sorted(outgoing, key=lambda item: item.ts):
        match = None
        for earlier in reversed(incoming_by_time):
            if earlier.txn_id in used or earlier.ts > payment.ts:
                continue
            if similar(earlier.amount, payment.amount, tolerance):
                match = earlier
                break
        if match is not None:
            used.add(match.txn_id)
            used.add(payment.txn_id)
    return used


def _residues(
    incoming: list[Payment],
    outgoing: list[Payment],
    used: set[int],
    currency: str,
    config: ResidueConfig,
) -> list[dict]:
    free_in = [payment for payment in incoming if payment.txn_id not in used]
    free_out = [payment for payment in outgoing if payment.txn_id not in used]
    found = []
    claimed: set[int] = set()
    for focal in sorted(free_out, key=lambda payment: payment.amount, reverse=True):
        if focal.txn_id in claimed:
            continue
        parts = _combine(
            [payment for payment in free_in if payment.ts <= focal.ts and payment.txn_id not in claimed],
            focal.amount,
            config,
        )
        if parts is None:
            continue
        claimed.add(focal.txn_id)
        claimed.update(payment.txn_id for payment in parts)
        found.append(_case("pooled", focal, parts, currency))
    for focal in sorted(free_in, key=lambda payment: payment.amount, reverse=True):
        if focal.txn_id in claimed:
            continue
        parts = _combine(
            [payment for payment in free_out if payment.ts >= focal.ts and payment.txn_id not in claimed],
            focal.amount,
            config,
        )
        if parts is None:
            continue
        claimed.add(focal.txn_id)
        claimed.update(payment.txn_id for payment in parts)
        found.append(_case("split", focal, parts, currency))
    return found


def _combine(candidates: list[Payment], target: float, config: ResidueConfig) -> list[Payment] | None:
    """Smallest set of 2..max_parts payments, from distinct counterparties, that sums to target."""
    ceiling = target * (1 - config.amount_tolerance)
    pool = [
        payment
        for payment in sorted(candidates, key=lambda payment: payment.ts, reverse=True)
        if 0 < payment.amount < ceiling
    ][: config.max_candidates]
    best: tuple[float, list[Payment]] | None = None
    for size in range(2, min(config.max_parts, len(pool)) + 1):
        for combo in combinations(pool, size):
            others = [payment.other for payment in combo]
            if len(set(others)) != len(others):
                continue
            total = sum(payment.amount for payment in combo)
            if not similar(total, target, config.sum_tolerance):
                continue
            gap = abs(total - target)
            if best is None or gap < best[0]:
                best = (gap, list(combo))
        if best is not None:
            return best[1]
    return None


def _case(kind: str, focal: Payment, parts: list[Payment], currency: str) -> dict:
    ordered = sorted(parts, key=lambda payment: payment.ts)
    return {
        "kind": kind,
        "focal_txn": focal.txn_id,
        "part_txns": [payment.txn_id for payment in ordered],
        "amount": focal.amount,
        "part_sum": sum(payment.amount for payment in ordered),
        "currency": currency,
        "ts": focal.ts,
    }


class ResidueScanner:
    def __init__(self, graph_db: GraphDB, config: ResidueConfig | None = None, case_graph: str = CASE_GRAPH):
        self.graph_db = graph_db
        self.config = config or ResidueConfig()
        self.case_graph = case_graph
        self._ready = False

    def scan_window(self, window: str) -> dict[str, int]:
        self._ensure()
        self.graph_db.ensure_indexes(window, [ENTITY_INDEX])
        counts = {"accounts": 0, "pooled": 0, "split": 0}
        skip = 0
        while True:
            result = self.graph_db.read(window, ACTIVE_ACCOUNTS, {"skip": skip, "limit": self.config.page})
            ids = [row[0] for row in result.result_set]
            if not ids:
                break
            counts["accounts"] += len(ids)
            for start in range(0, len(ids), self.config.account_chunk):
                chunk = ids[start : start + self.config.account_chunk]
                eligible = [
                    row[0]
                    for row in self.graph_db.read(
                        window, ELIGIBLE, {"ids": chunk, "spread": self.config.spread_limit}
                    ).result_set
                ]
                if not eligible:
                    continue
                edges = self.graph_db.read(window, EDGES, {"ids": eligible}).result_set
                by_account: dict[str, list[Payment]] = defaultdict(list)
                for account, direction, other, txn_id, ts, amount, currency in edges:
                    by_account[account].append(
                        Payment(int(txn_id), int(ts), float(amount), currency, other, direction)
                    )
                cases = []
                for account, payments in by_account.items():
                    for case in explain_account(payments, self.config):
                        part_txns = ",".join(str(txn_id) for txn_id in case["part_txns"])
                        cases.append(
                            {
                                "id": f"{case['kind']}:{window}:{account}:{case['focal_txn']}",
                                "kind": case["kind"],
                                "account": account,
                                "window": window,
                                "focal_txn": case["focal_txn"],
                                "part_txns": part_txns,
                                "amount": case["amount"],
                                "part_sum": case["part_sum"],
                                "currency": case["currency"],
                                "ts": case["ts"],
                            }
                        )
                        counts[case["kind"]] += 1
                self._write(cases)
            skip += len(ids)
            logger.info(
                "%s accounts=%s pooled=%s split=%s",
                window,
                f"{counts['accounts']:,}",
                f"{counts['pooled']:,}",
                f"{counts['split']:,}",
            )
        return counts

    def _write(self, cases: list[dict]) -> None:
        for start in range(0, len(cases), 200):
            self.graph_db.query(self.case_graph, WRITE_CASES, {"cases": cases[start : start + 200]})

    def _ensure(self) -> None:
        if self._ready:
            return
        self.graph_db.ensure_indexes(self.case_graph, CASE_INDEXES)
        self._ready = True
