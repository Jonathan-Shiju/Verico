"""A temporary Falkor graph of the payments a reason cites.

The graph lives only for the call. Payments that share an account come back
as one graph. Payments that never meet come back as separate graphs.
"""

from uuid import uuid4

from GraphRead.reader import GraphReader, _rows

MAX_TRANSACTIONS = 40

# Both ends stay unlabeled. Labeling them makes Falkor scan every account
# before it uses the transaction-id index, and that scan times out.
FIND_TRANSFER = """
MATCH (a)-[t:TRANSFER]->(b)
WHERE t.txn_id = $id
RETURN a.id AS src, b.id AS dst, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
LIMIT 1
"""

ENTITY = """
MATCH (a:Account {id: $id})
RETURN a.entity_id AS entity_id, a.entity_name AS entity_name
LIMIT 1
"""

WRITE_ACCOUNTS = """
UNWIND $accounts AS n
MERGE (a:Account {id: n.id})
  ON CREATE SET a.entity_id = n.entity_id, a.entity_name = n.entity_name
"""

WRITE_TRANSFERS = """
UNWIND $rows AS r
MATCH (a:Account {id: r.src}), (b:Account {id: r.dst})
MERGE (a)-[t:TRANSFER {txn_id: r.txn_id}]->(b)
  ON CREATE SET t.ts = r.ts,
                t.amount_paid = r.amount_paid,
                t.payment_currency = r.payment_currency,
                t.amount_received = r.amount_received,
                t.receiving_currency = r.receiving_currency,
                t.payment_format = r.payment_format
"""

READ_GRAPH = """
MATCH (a:Account)-[t:TRANSFER]->(b:Account)
RETURN a.id AS src, a.entity_id AS src_entity_id, a.entity_name AS src_entity_name,
       b.id AS dst, b.entity_id AS dst_entity_id, b.entity_name AS dst_entity_name,
       t.txn_id AS txn_id, t.ts AS ts, t.amount_paid AS amount_paid,
       t.payment_currency AS payment_currency, t.amount_received AS amount_received,
       t.receiving_currency AS receiving_currency, t.payment_format AS payment_format
"""


def explain_transactions(reader: GraphReader, txn_ids: list[int]) -> dict:
    """Load these payments into a temporary graph and return each connected piece."""
    try:
        wanted = _txn_ids(txn_ids)
    except (TypeError, ValueError):
        return {"error": "txn_ids must be transaction ids"}
    if not wanted:
        return {"error": "Name the transaction ids to explain"}
    if len(wanted) > MAX_TRANSACTIONS:
        return {"error": f"At most {MAX_TRANSACTIONS} transaction ids"}

    found = []
    missing = []
    windows = [item["graph"] for item in reader.list_windows()["windows"]]
    for txn_id in wanted:
        transfer = _find_transfer(reader, windows, txn_id)
        if transfer is None:
            missing.append(txn_id)
        else:
            found.append(transfer)
    if not found:
        return {"graphs": [], "missing": missing}

    accounts = _accounts(reader, found)
    graph = f"aml:tmp:{uuid4().hex}"
    try:
        reader.graph_db.query(graph, WRITE_ACCOUNTS, {"accounts": list(accounts.values())})
        reader.graph_db.query(
            graph,
            WRITE_TRANSFERS,
            {"rows": [_write_row(transfer) for transfer in found]},
        )
        stored = _rows(reader.graph_db.read(graph, READ_GRAPH), MAX_TRANSACTIONS)["rows"]
    finally:
        reader.graph_db.delete_graph(graph)

    graphs = _components(stored, {transfer["txn_id"]: transfer["window"] for transfer in found})
    return {"graphs": graphs, "missing": missing}


def _txn_ids(raw) -> list[int]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        values = list(raw)
    seen = []
    for value in values:
        txn_id = int(value)
        if txn_id not in seen:
            seen.append(txn_id)
    return seen


def _find_transfer(reader: GraphReader, windows: list[str], txn_id: int) -> dict | None:
    for window in windows:
        rows = _rows(
            reader.graph_db.read(window, FIND_TRANSFER, {"id": txn_id}),
            1,
        )["rows"]
        if rows:
            rows[0]["window"] = window
            return rows[0]
    return None


def _accounts(reader: GraphReader, transfers: list[dict]) -> dict[str, dict]:
    accounts = {}
    for transfer in transfers:
        for account in (transfer["src"], transfer["dst"]):
            if account in accounts:
                continue
            entity_id, entity_name = None, None
            rows = reader.graph_db.read(
                transfer["window"], ENTITY, {"id": account}
            ).result_set
            if rows:
                entity_id, entity_name = rows[0]
            accounts[account] = {
                "id": account,
                "entity_id": entity_id,
                "entity_name": entity_name,
            }
    return accounts


def _write_row(transfer: dict) -> dict:
    return {
        "txn_id": int(transfer["txn_id"]),
        "src": transfer["src"],
        "dst": transfer["dst"],
        "ts": int(transfer["ts"]),
        "amount_paid": transfer["amount_paid"],
        "payment_currency": transfer["payment_currency"],
        "amount_received": transfer["amount_received"],
        "receiving_currency": transfer["receiving_currency"],
        "payment_format": transfer["payment_format"],
    }


def _components(rows: list[dict], windows: dict[int, str]) -> list[dict]:
    parent: dict[str, str] = {}

    def find(account: str) -> str:
        parent.setdefault(account, account)
        while parent[account] != account:
            parent[account] = parent[parent[account]]
            account = parent[account]
        return account

    def unite(left: str, right: str) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    for row in rows:
        unite(row["src"], row["dst"])

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(find(row["src"]), []).append(row)

    graphs = []
    for members in grouped.values():
        members.sort(key=lambda row: (row.get("ts") or 0, row["txn_id"]))
        accounts: dict[str, dict] = {}
        transfers = []
        for row in members:
            accounts.setdefault(
                row["src"],
                {
                    "id": row["src"],
                    "entity_id": row.get("src_entity_id"),
                    "entity_name": row.get("src_entity_name"),
                },
            )
            accounts.setdefault(
                row["dst"],
                {
                    "id": row["dst"],
                    "entity_id": row.get("dst_entity_id"),
                    "entity_name": row.get("dst_entity_name"),
                },
            )
            transfers.append(
                {
                    "txn_id": int(row["txn_id"]),
                    "src": row["src"],
                    "dst": row["dst"],
                    "amount_paid": row.get("amount_paid"),
                    "payment_currency": row.get("payment_currency"),
                    "amount_received": row.get("amount_received"),
                    "receiving_currency": row.get("receiving_currency"),
                    "payment_format": row.get("payment_format"),
                    "time": row.get("time"),
                    "window": windows.get(int(row["txn_id"])),
                }
            )
        graphs.append(
            {
                "accounts": [accounts[name] for name in sorted(accounts)],
                "transfers": transfers,
            }
        )
    graphs.sort(key=lambda graph: (graph["transfers"][0].get("time") or "", graph["transfers"][0]["txn_id"]))
    return graphs
