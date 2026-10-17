"""What an operator asks for before the other tools.

A lead is a stored loop or residue. A counterparty lead is separate and is
written only when the operator asks. Opening any of them with case_file
produces the case file. A summary is every payment on one
account, or the accounts linked by one entity. A walk starts at one payment
and follows later outgoing payments from the account that received it.
"""

from collections import deque

import redis

from Detection.ResidueCases import CASE_GRAPH
from GraphRead.explain import _find_transfer
from GraphRead.reader import LOOP_GRAPH, TRANSFER_PAGE, GraphReader, _page_query, _rows
from GraphRead.reviews import LIST_REVIEWS, REVIEW_GRAPH

LEAD_PAGE = 3
WALK_DEPTH = 3
WALK_CHILDREN = 8
WALK_EDGES = 24
ENTITY_ACCOUNTS = 20
# Pulling every payment for a party larger than this is a scan, not a summary.
ENTITY_PAYMENT_ACCOUNTS = 8

LIST_LOOPS = """
MATCH (l:Loop)
RETURN l.key AS lead, 'loop' AS kind, l.length AS length, l.open AS open,
       l.start_ts AS start_ts, l.end_ts AS end_ts, l.tree_root AS account
ORDER BY l.start_ts
SKIP $skip
LIMIT $limit
"""

LIST_CASES = """
MATCH (c:Case)
RETURN c.id AS lead, c.kind AS kind, c.account AS account, c.window AS window,
       c.focal_txn AS focal_txn, c.ts AS ts
SKIP $skip
LIMIT $limit
"""

ENTITY_PAGE = """
MATCH (a:Account {entity_id: $entity})
RETURN a.id AS id, a.entity_name AS entity_name
LIMIT $limit
"""

ENTITY_COUNT = """
MATCH (a:Account {entity_id: $entity})
RETURN count(a) AS accounts
"""


def list_leads(reader: GraphReader, kind: str = "all", skip: int = 0, limit: int = LEAD_PAGE) -> dict:
    """One page of leads an operator can open.

    kind is loop, residue, review, or all. review is omitted from all.
    Pass a returned lead to case_file to open it.
    """
    kind = (kind or "all").strip().lower()
    if kind not in {"all", "loop", "residue", "review"}:
        return {"error": "kind is loop, residue, review, or all"}
    skip = max(0, skip)
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = LEAD_PAGE
    limit = max(1, min(limit, LEAD_PAGE))
    leads = []
    if kind in {"all", "loop"}:
        leads.extend(_stored_page(reader, LOOP_GRAPH, LIST_LOOPS, skip, limit, "loop"))
    if kind in {"all", "residue"}:
        leads.extend(_stored_page(reader, CASE_GRAPH, LIST_CASES, skip, limit, "residue"))
    if kind == "review":
        leads.extend(_stored_page(reader, REVIEW_GRAPH, LIST_REVIEWS, skip, limit, "review"))
    return {
        "kind": kind,
        "skip": skip,
        "leads": leads,
        "truncated": len(leads) >= limit if kind != "all" else any(
            sum(1 for lead in leads if lead["source"] == source) >= limit
            for source in ("loop", "residue")
        ),
    }


def summarize(reader: GraphReader, account_id: str = "", entity_id: str = "") -> dict:
    """Payments on one account, or the accounts that share an entity."""
    if account_id and entity_id:
        return {"error": "Ask for an account or an entity, not both"}
    if account_id:
        transfers = reader.account_transfers(account_id)
        return {
            "scope": "account",
            "account": account_id,
            "entity": transfers["entity"],
            "transfers": transfers["transfers"],
            "complete": transfers["complete"],
        }
    if entity_id:
        return _entity_summary(reader, entity_id)
    return {"error": "Name an account or an entity"}


def walk_transaction(reader: GraphReader, txn_id: int, depth: int = WALK_DEPTH) -> dict:
    """Follow later outgoing payments from the account that received this one.

    Each hop starts at an account. The time filter is applied after that read,
    because a time comparison in the query scans the window.
    """
    try:
        txn_id = int(txn_id)
    except (TypeError, ValueError):
        return {"error": "txn_id must be a transaction id"}
    depth = max(1, min(int(depth), 4))
    windows = [item["graph"] for item in reader.list_windows()["windows"]]
    root = _find_transfer(reader, windows, txn_id)
    if root is None:
        return {"txn_id": txn_id, "error": "No payment with that id"}
    later = windows[windows.index(root["window"]) :] if root["window"] in windows else windows
    steps = []
    seen = 0
    truncated = False
    expanded = set()
    waiting = deque([(root["dst"], int(root["ts"]), 1)])
    while waiting and seen < WALK_EDGES:
        account, after, level = waiting.popleft()
        if level > depth or account in expanded:
            continue
        expanded.add(account)
        outgoing = _outgoing_after(reader, later, account, after)
        if outgoing is None:
            truncated = True
            break
        if not outgoing:
            continue
        room = WALK_EDGES - seen
        if len(outgoing) > room:
            outgoing = outgoing[:room]
            truncated = True
        steps.append({"depth": level, "from_account": account, "transfers": outgoing})
        seen += len(outgoing)
        if level < depth:
            for transfer in outgoing:
                waiting.append((transfer["other"], int(transfer["ts"]), level + 1))
    return {
        "txn_id": txn_id,
        "root": _root(root),
        "depth": depth,
        "steps": steps,
        "truncated": truncated,
    }


def _stored_page(
    reader: GraphReader, graph: str, query: str, skip: int, limit: int, source: str
) -> list[dict]:
    """A missing graph is an empty page. Loops can exist before cases or reviews."""
    try:
        return _lead_page(reader, graph, query, skip, limit, source)
    except redis.ResponseError:
        return []


def _lead_page(
    reader: GraphReader, graph: str, query: str, skip: int, limit: int, source: str
) -> list[dict]:
    rows = _rows(reader.graph_db.read(graph, query, {"skip": skip, "limit": limit}), limit)["rows"]
    for row in rows:
        row["source"] = source
    return rows


def _entity_summary(reader: GraphReader, entity_id: str) -> dict:
    window = reader.list_windows()["windows"][0]["graph"]
    counted = reader.graph_db.read(window, ENTITY_COUNT, {"entity": entity_id}).result_set
    if not counted or counted[0][0] in (0, None):
        return {"scope": "entity", "entity_id": entity_id, "error": "No accounts for that entity"}
    account_count = int(counted[0][0])
    listed = _rows(
        reader.graph_db.read(window, ENTITY_PAGE, {"entity": entity_id, "limit": ENTITY_ACCOUNTS}),
        ENTITY_ACCOUNTS,
    )["rows"]
    accounts = [row["id"] for row in listed]
    entity_name = listed[0]["entity_name"] if listed else None
    summary = {
        "scope": "entity",
        "entity_id": entity_id,
        "entity_name": entity_name,
        "account_count": account_count,
        "accounts": accounts,
        "accounts_truncated": account_count > len(accounts),
    }
    if account_count > ENTITY_PAYMENT_ACCOUNTS:
        summary["transfers"] = []
        summary["complete"] = False
        summary["note"] = "Ask for one of these accounts to read its payments."
        return summary
    transfers = []
    seen = set()
    complete = True
    for account in accounts:
        page = reader.account_transfers(account)
        complete = complete and page["complete"]
        for transfer in page["transfers"]:
            if transfer["txn_id"] in seen:
                continue
            seen.add(transfer["txn_id"])
            transfers.append(transfer)
    transfers.sort(key=lambda row: (row.get("ts") or 0, row["txn_id"]))
    summary["transfers"] = transfers
    summary["complete"] = complete
    return summary


def _outgoing_after(reader: GraphReader, windows: list[str], account: str, after: int) -> list[dict] | None:
    query = _page_query("out", outgoing=True, include_self=False)
    found = []
    seen = set()
    try:
        for graph in windows:
            skip = 0
            while len(found) < WALK_CHILDREN:
                rows = _rows(
                    reader.graph_db.read(
                        graph,
                        query,
                        {"id": account, "skip": skip, "limit": TRANSFER_PAGE},
                    ),
                    TRANSFER_PAGE,
                )["rows"]
                for row in rows:
                    if int(row["ts"]) < after or row["txn_id"] in seen:
                        continue
                    seen.add(row["txn_id"])
                    row["window"] = graph
                    found.append(row)
                    if len(found) >= WALK_CHILDREN:
                        break
                if len(rows) < TRANSFER_PAGE:
                    break
                skip += TRANSFER_PAGE
    except redis.ResponseError:
        return None
    found.sort(key=lambda row: (row.get("ts") or 0, row["txn_id"]))
    return found[:WALK_CHILDREN]


def _root(transfer: dict) -> dict:
    return {
        "txn_id": int(transfer["txn_id"]),
        "src": transfer["src"],
        "dst": transfer["dst"],
        "amount_paid": transfer.get("amount_paid"),
        "payment_currency": transfer.get("payment_currency"),
        "payment_format": transfer.get("payment_format"),
        "time": transfer.get("time"),
        "window": transfer.get("window"),
    }
