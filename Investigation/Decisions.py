"""Keep or dismiss for one lead, stored outside the transaction windows."""

import time

from adapters.graphdb import GraphDB

DECISION_GRAPH = "aml:decisions"

DECISION_INDEXES = [
    "CREATE INDEX FOR (d:Decision) ON (d.lead)",
]

WRITE_DECISION = """
MERGE (d:Decision {lead: $lead})
SET d.decision = $decision,
    d.kind = $kind,
    d.reason = $reason,
    d.accounts = $accounts,
    d.txn_ids = $txn_ids,
    d.recorded_ts = $recorded_ts
"""

READ_DECISION = """
MATCH (d:Decision {lead: $lead})
RETURN d.lead AS lead, d.decision AS decision, d.kind AS kind,
       d.reason AS reason, d.accounts AS accounts, d.txn_ids AS txn_ids,
       d.recorded_ts AS recorded_ts
"""


class DecisionStore:
    def __init__(self, graph_db: GraphDB):
        self.graph_db = graph_db
        self._ready = False

    def ensure(self) -> None:
        if self._ready:
            return
        self.graph_db.ensure_indexes(DECISION_GRAPH, DECISION_INDEXES)
        self._ready = True

    def write(
        self,
        lead: str,
        decision: str,
        kind: str,
        reason: str,
        accounts: list[str],
        txn_ids: list[int],
    ) -> dict:
        self.ensure()
        recorded_ts = int(time.time())
        self.graph_db.query(
            DECISION_GRAPH,
            WRITE_DECISION,
            {
                "lead": lead,
                "decision": decision,
                "kind": kind,
                "reason": reason,
                "accounts": accounts,
                "txn_ids": txn_ids,
                "recorded_ts": recorded_ts,
            },
        )
        return {
            "lead": lead,
            "decision": decision,
            "kind": kind,
            "reason": reason,
            "accounts": accounts,
            "txn_ids": txn_ids,
            "recorded_ts": recorded_ts,
        }
