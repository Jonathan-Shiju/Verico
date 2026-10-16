"""Counterparty leads an operator orders opened.

These are not loops and not residues. One flagged account gets a lead for
each account it paid or that paid it. The lead is stored in its own graph.
case_file opens that lead into the payments between the two accounts.
"""

from GraphIngest.Windows import format_ts

REVIEW_GRAPH = "aml:reviews"
REVIEW_LIMIT = 12
PAYMENTS_PER_LEAD = 8
REVIEW_INDEXES = [
    "CREATE INDEX FOR (r:Review) ON (r.id)",
    "CREATE INDEX FOR (r:Review) ON (r.account)",
]

MERGE_REVIEW = """
MERGE (r:Review {id: $id})
SET r.kind = 'counterparty', r.account = $account, r.counterparty = $counterparty,
    r.txn_ids = $txn_ids, r.window = $window, r.ts = $ts
RETURN r.id AS lead
"""

LIST_REVIEWS = """
MATCH (r:Review)
RETURN r.id AS lead, 'counterparty' AS kind, r.account AS account,
       r.counterparty AS counterparty, r.ts AS ts
ORDER BY r.ts
SKIP $skip
LIMIT $limit
"""

READ_REVIEW = """
MATCH (r:Review {id: $id})
RETURN r.id AS id, r.kind AS kind, r.account AS account, r.counterparty AS counterparty,
       r.txn_ids AS txn_ids, r.window AS window, r.ts AS ts
"""


def open_counterparty_leads(reader, account_id: str, limit: int = REVIEW_LIMIT) -> dict:
    """Open one lead per counterparty of the account the operator named.

    The payments are the ones already on that account. Loops and residues
    are left as they are.
    """
    account_id = (account_id or "").strip()
    if not account_id:
        return {"error": "Name the account to flag"}
    limit = max(1, min(int(limit), 25))
    page = reader.account_transfers(account_id)
    if page.get("error"):
        return page
    grouped = _by_counterparty(page.get("transfers") or [])
    if not grouped:
        return {"account": account_id, "error": "That account has no payments to open"}
    _ensure(reader)
    account_name = (page.get("entity") or {}).get("entity_name") or ""
    opened = []
    for counterparty, payments in list(grouped.items())[:limit]:
        cited = payments[:PAYMENTS_PER_LEAD]
        lead = f"review:{account_id}:{counterparty}"
        first = cited[0]
        reader.graph_db.query(
            REVIEW_GRAPH,
            MERGE_REVIEW,
            {
                "id": lead,
                "account": account_id,
                "counterparty": counterparty,
                "txn_ids": ",".join(str(row["txn_id"]) for row in cited),
                "window": first.get("window") or "",
                "ts": int(first["ts"]) if first.get("ts") is not None else 0,
            },
        )
        other = reader._entity(counterparty, first.get("window"))
        other_name = other.get("entity_name") or ""
        opened.append(
            {
                "lead": lead,
                "kind": "counterparty",
                "counterparty": counterparty,
                "name": other_name,
                "txn_ids": [int(row["txn_id"]) for row in cited],
                "payments": len(cited),
                "payments_truncated": len(payments) > len(cited),
                "lines": [
                    _line(row, account_name or account_id, other_name or counterparty)
                    for row in cited
                ],
            }
        )
    return {
        "account": account_id,
        "name": account_name,
        "kind": "counterparty",
        "leads": opened,
        "truncated": len(grouped) > limit,
        "complete": page.get("complete", True),
    }


def _line(row: dict, account_name: str, other_name: str) -> dict:
    sent = row.get("direction") == "out"
    amount = row.get("amount_paid") if sent else row.get("amount_received") or row.get("amount_paid")
    currency = (
        row.get("payment_currency")
        if sent
        else row.get("receiving_currency") or row.get("payment_currency") or ""
    )
    return {
        "time": format_ts(int(row["ts"])) if row.get("ts") is not None else "",
        "amount": amount,
        "currency": currency or "",
        "format": row.get("payment_format") or "",
        "from": account_name if sent else other_name,
        "to": other_name if sent else account_name,
    }


def _by_counterparty(transfers: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for transfer in transfers:
        other = transfer.get("other")
        if not other:
            continue
        grouped.setdefault(other, []).append(transfer)
    return grouped


def _ensure(reader) -> None:
    reader.graph_db.ensure_indexes(REVIEW_GRAPH, REVIEW_INDEXES)
