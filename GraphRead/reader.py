"""Read-only views over the window graphs and the loop graph.

Windows are separate FalkorDB graphs. The same account id is what joins them:
a one-hop counterparty in one window is looked up by that id in the windows
on either side.
"""

import json

import redis

from adapters.graphdb import GraphDB
from GraphIngest.Windows import WindowSpec, format_ts, parse_ts
from Detection.ResidueCases import CASE_GRAPH
from GraphRead.reviews import READ_REVIEW, REVIEW_GRAPH
from GraphIngest.WindowFlags import FLAG_GRAPH
from Investigation.Decisions import DECISION_GRAPH, READ_DECISION, DecisionStore

LOOP_GRAPH = "aml:loops"
ENTITY_LIMIT = 15

FLAGS_FOR_ACCOUNT = """
MATCH (f:Flag {account: $id})
RETURN f.rule AS rule, f.window AS window, f.parties AS parties,
       f.txn_id AS txn_id, f.amount AS amount, f.inflow_sum AS inflow_sum,
       f.inflow_txns AS inflow_txns, f.ts AS ts
LIMIT $limit
"""
MAX_ROWS = 200

# A parameter in the self-transfer check makes Falkor scan the window.
# The comparison is written out so the read stays on the one account.
_TRANSFER_FIELDS = """
RETURN 'out' AS direction, b.id AS other, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
ORDER BY t.ts
LIMIT $limit
UNION
MATCH (b:Account)-[t:TRANSFER]->(a:Account {id: $id})
"""

TRANSFERS = f"""
MATCH (a:Account {{id: $id}})-[t:TRANSFER]->(b:Account)
WHERE a.id <> b.id
{_TRANSFER_FIELDS}
WHERE a.id <> b.id
RETURN 'in' AS direction, b.id AS other, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
ORDER BY t.ts
LIMIT $limit
"""

TRANSFERS_WITH_SELF = f"""
MATCH (a:Account {{id: $id}})-[t:TRANSFER]->(b:Account)
{_TRANSFER_FIELDS}
RETURN 'in' AS direction, b.id AS other, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
ORDER BY t.ts
LIMIT $limit
"""

# One Falkor reply is capped, so each direction of an account is read a page at a time.
TRANSFER_PAGE = 200

LOOPS_FOR_ACCOUNT = """
MATCH (a:Account {id: $id})-[r:IN_LOOP]->(l:Loop)
RETURN l.key AS key, r.position AS position, l.length AS length,
       l.start_ts AS start_ts, l.end_ts AS end_ts,
       l.amount_out AS amount_out, l.amount_back AS amount_back,
       l.tree_root AS tree_root, l.closed_at_root AS closed_at_root,
       l.open AS open, l.txn_ids AS txn_ids
ORDER BY l.start_ts
LIMIT $limit
"""

CASES_FOR_ACCOUNT = """
MATCH (c:Case {account: $id})
RETURN c.kind AS kind, c.window AS window, c.focal_txn AS focal_txn,
       c.part_txns AS part_txns, c.amount AS amount, c.part_sum AS part_sum,
       c.currency AS currency, c.ts AS ts
ORDER BY c.ts
LIMIT $limit
"""

ENTITY = """
MATCH (a:Account {id: $id})
RETURN a.entity_id AS entity_id, a.entity_name AS entity_name
LIMIT 1
"""

ENTITY_ACCOUNTS = """
MATCH (a:Account {entity_id: $entity})
WHERE a.id <> $id
RETURN a.id AS id
LIMIT $limit
"""

ENTITY_COUNT = """
MATCH (a:Account {entity_id: $entity})
RETURN count(a) AS accounts
"""

CHAIN_LEAD = """
MATCH (a:Account)-[r:IN_LOOP]->(l:Loop {key: $key})
RETURN l.key AS key, l.txn_ids AS txn_ids, l.length AS length,
       l.start_ts AS start_ts, l.end_ts AS end_ts,
       l.amount_out AS amount_out, l.amount_back AS amount_back,
       l.tree_root AS tree_root, l.closed_at_root AS closed_at_root,
       l.open AS open, a.id AS account, r.position AS position
ORDER BY r.position
"""

RESIDUE_LEAD = """
MATCH (c:Case {id: $id})
RETURN c.id AS id, c.kind AS kind, c.account AS account, c.window AS window,
       c.focal_txn AS focal_txn, c.part_txns AS part_txns, c.amount AS amount,
       c.part_sum AS part_sum, c.currency AS currency, c.ts AS ts
"""

# Start at a known account. An unbound scan on txn_id times out on a full window.
ACCOUNT_EDGES = """
MATCH (a:Account {id: $id})-[t:TRANSFER]->(b:Account)
RETURN a.id AS src, b.id AS dst, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
LIMIT $limit
UNION
MATCH (b:Account)-[t:TRANSFER]->(a:Account {id: $id})
RETURN b.id AS src, a.id AS dst, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
LIMIT $limit
"""


def _transfer_pages(include_self: bool) -> tuple[str, str]:
    return (
        _page_query("out", outgoing=True, include_self=include_self),
        _page_query("in", outgoing=False, include_self=include_self),
    )


def _page_query(direction: str, outgoing: bool, include_self: bool) -> str:
    match = (
        "MATCH (a:Account {id: $id})-[t:TRANSFER]->(b:Account)"
        if outgoing
        else "MATCH (b:Account)-[t:TRANSFER]->(a:Account {id: $id})"
    )
    where = "" if include_self else "\nWHERE a.id <> b.id"
    return f"""
{match}{where}
RETURN '{direction}' AS direction, b.id AS other, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.amount_received AS amount_received, t.receiving_currency AS receiving_currency,
       t.payment_format AS payment_format
ORDER BY t.ts, t.txn_id
SKIP $skip
LIMIT $limit
"""


def _txn_id_list(raw) -> list[int]:
    if isinstance(raw, str):
        return [int(txn_id) for txn_id in raw.split(",") if txn_id]
    return [int(txn_id) for txn_id in raw]


def _rows(result, limit: int) -> dict:
    names = []
    for column in result.header:
        names.append(column[1] if isinstance(column, (list, tuple)) else column)
    rows = []
    for record in result.result_set[:limit]:
        row = {}
        for name, value in zip(names, record):
            if name == "ts" or name.endswith("_ts"):
                row[name] = value
                row[name.replace("_ts", "_time") if name != "ts" else "time"] = (
                    format_ts(int(value)) if value is not None else None
                )
            else:
                row[name] = value
        rows.append(row)
    return {
        "columns": names,
        "rows": rows,
        "truncated": len(result.result_set) > limit,
    }


class GraphReader:
    def __init__(self, graph_db: GraphDB, windows: WindowSpec | None = None):
        self.graph_db = graph_db
        self.windows = windows or WindowSpec(8 * 86400, 4 * 86400)

    def list_windows(self) -> dict:
        marks = self.graph_db.get_state(self.windows.watermark_key)
        resolved = marks.get("resolved")
        windows = []
        for name, raw in self.graph_db.get_state(self.windows.windows_key).items():
            window = json.loads(raw)
            windows.append(
                {
                    "graph": name,
                    "start": format_ts(window["start"]),
                    "end": format_ts(window["end"]),
                    "status": window["status"],
                }
            )
        windows.sort(key=lambda item: item["start"])
        return {
            "watermark": format_ts(int(resolved)) if resolved else None,
            "windows": windows,
            "loop_graph": LOOP_GRAPH,
            "case_graph": CASE_GRAPH,
            "account_id": "{bank}:{account}, bank written without leading zeros",
        }

    def query(self, graph: str, cypher: str, limit: int = 100) -> dict:
        self._check_graph(graph)
        limit = max(1, min(limit, MAX_ROWS))
        try:
            result = self.graph_db.read(graph, cypher)
        except redis.ResponseError as err:
            return {"graph": graph, "error": str(err)}
        payload = _rows(result, limit)
        payload["graph"] = graph
        return payload

    def account_activity(
        self,
        account_id: str,
        window: str | None = None,
        limit: int = 50,
        include_self: bool = False,
    ) -> dict:
        """Transfers for one account. With no window, every window is searched,
        which is how the same account is followed from one graph to the next.

        Self-transfers are omitted unless include_self is set. The entity id
        links other accounts of the same party; the list is capped.
        """
        limit = max(1, min(limit, MAX_ROWS))
        graphs = [window] if window else [item["graph"] for item in self.list_windows()["windows"]]
        activity = []
        timed_out = []
        for graph in graphs:
            self._check_graph(graph)
            try:
                rows = _rows(
                    self.graph_db.read(
                        graph,
                        TRANSFERS_WITH_SELF if include_self else TRANSFERS,
                        {"id": account_id, "limit": limit},
                    ),
                    limit,
                )["rows"]
            except redis.ResponseError:
                timed_out.append(graph)
                continue
            if rows:
                activity.append({"graph": graph, "transfers": rows})
        entity_window = window or (graphs[0] if graphs else None)
        try:
            entity = self._entity(account_id, entity_window)
        except redis.ResponseError:
            entity = {"entity_id": None, "entity_name": None, "accounts": 0, "other_accounts": []}
        result = {
            "account": account_id,
            "include_self": include_self,
            "entity": entity,
            "windows": activity,
        }
        if timed_out:
            result["complete"] = False
        return result

    def account_transfers(
        self,
        account_id: str,
        window: str | None = None,
        include_self: bool = False,
    ) -> dict:
        """Every payment on one account. Falkor caps one reply, so this pages.

        With no window, every window is searched. A payment stored in two
        overlapping windows is returned once. complete is false when a page
        timed out before the account was finished.
        """
        graphs = [window] if window else [item["graph"] for item in self.list_windows()["windows"]]
        found = []
        seen = set()
        complete = True
        for graph in graphs:
            self._check_graph(graph)
            for query in _transfer_pages(include_self):
                skip = 0
                while True:
                    try:
                        rows = _rows(
                            self.graph_db.read(
                                graph,
                                query,
                                {"id": account_id, "skip": skip, "limit": TRANSFER_PAGE},
                            ),
                            TRANSFER_PAGE,
                        )["rows"]
                    except redis.ResponseError:
                        complete = False
                        break
                    for row in rows:
                        if row["txn_id"] in seen:
                            continue
                        seen.add(row["txn_id"])
                        row["window"] = graph
                        found.append(row)
                    if len(rows) < TRANSFER_PAGE:
                        break
                    skip += TRANSFER_PAGE
        found.sort(key=lambda row: (row.get("ts") or 0, row["txn_id"]))
        entity_window = window or (graphs[0] if graphs else None)
        return {
            "account": account_id,
            "include_self": include_self,
            "complete": complete,
            "transfers": found,
            "entity": self._entity(account_id, entity_window),
        }

    def one_hop_across_windows(
        self,
        account_id: str,
        window: str,
        counterparty_limit: int = 10,
        limit: int = 30,
    ) -> dict:
        """Counterparties of an account in one window, then those same accounts
        in the previous and next windows."""
        self._check_graph(window)
        limit = max(1, min(limit, MAX_ROWS))
        counterparty_limit = max(1, min(counterparty_limit, 25))
        focal = self.account_activity(account_id, window, limit)
        others = []
        seen = set()
        for entry in focal["windows"]:
            for transfer in entry["transfers"]:
                other = transfer["other"]
                if other not in seen:
                    seen.add(other)
                    others.append(other)
                if len(others) >= counterparty_limit:
                    break
        adjacent = self._adjacent(window)
        followed = []
        for other in others:
            hops = []
            for graph in adjacent:
                rows = self.account_activity(other, graph, limit)["windows"]
                if rows:
                    hops.append(rows[0])
            if hops:
                followed.append({"account": other, "adjacent_windows": hops})
        return {
            "account": account_id,
            "window": window,
            "adjacent_windows": adjacent,
            "activity": focal["windows"],
            "counterparties": followed,
        }

    def loops_for_account(self, account_id: str, limit: int = 20) -> dict:
        limit = max(1, min(limit, MAX_ROWS))
        payload = _rows(
            self.graph_db.read(LOOP_GRAPH, LOOPS_FOR_ACCOUNT, {"id": account_id, "limit": limit}),
            limit,
        )
        payload["account"] = account_id
        payload["graph"] = LOOP_GRAPH
        return payload

    def cases_for_account(self, account_id: str, limit: int = 20) -> dict:
        """Pooled and split residues stored for an account after one-to-one matching."""
        limit = max(1, min(limit, MAX_ROWS))
        payload = _rows(
            self.graph_db.read(CASE_GRAPH, CASES_FOR_ACCOUNT, {"id": account_id, "limit": limit}),
            limit,
        )
        payload["account"] = account_id
        payload["graph"] = CASE_GRAPH
        return payload

    def case_file(self, lead: str) -> dict:
        """One file for a model call: the lead's payments, accounts, entities, and windows.

        `lead` is a loop or open-chain key (comma-separated transaction ids), a
        residue id such as pooled:{window}:{account}:{txn}, or a counterparty
        lead id such as review:{account}:{counterparty}.
        """
        if lead.startswith("pooled:") or lead.startswith("split:"):
            return self._residue_file(lead)
        if lead.startswith("review:"):
            return self._review_file(lead)
        return self._chain_file(lead)

    def decision_for_lead(self, lead: str) -> dict:
        """The keep or dismiss stored for a lead, outside the transaction windows."""
        DecisionStore(self.graph_db).ensure()
        rows = _rows(
            self.graph_db.read(DECISION_GRAPH, READ_DECISION, {"lead": lead}),
            1,
        )["rows"]
        if not rows:
            return {"lead": lead, "graph": DECISION_GRAPH, "decision": None}
        row = rows[0]
        row["graph"] = DECISION_GRAPH
        row["accounts"] = [str(account) for account in row.get("accounts") or []]
        row["txn_ids"] = [int(txn_id) for txn_id in row.get("txn_ids") or []]
        return row

    def flags_for_account(self, account_id: str, limit: int = 20) -> dict:
        limit = max(1, min(limit, MAX_ROWS))
        payload = _rows(
            self.graph_db.read(FLAG_GRAPH, FLAGS_FOR_ACCOUNT, {"id": account_id, "limit": limit}),
            limit,
        )
        payload["account"] = account_id
        payload["graph"] = FLAG_GRAPH
        return payload

    def _chain_file(self, key: str) -> dict:
        rows = self.graph_db.read(LOOP_GRAPH, CHAIN_LEAD, {"key": key}).result_set
        if not rows:
            return {"lead": key, "error": "No loop or open chain with that key"}
        (
            _key,
            raw_txns,
            length,
            start_ts,
            end_ts,
            amount_out,
            amount_back,
            tree_root,
            closed_at_root,
            is_open,
            _account,
            _position,
        ) = rows[0]
        txn_ids = _txn_id_list(raw_txns)
        accounts = [row[10] for row in rows]
        start_ts, end_ts = int(start_ts), int(end_ts)
        windows = self._windows_covering(start_ts, end_ts)
        return {
            "lead": key,
            "kind": "open_chain" if is_open else "loop",
            "open": bool(is_open),
            "closed_at_root": bool(closed_at_root),
            "tree_root": tree_root,
            "length": int(length),
            "start_time": format_ts(start_ts),
            "end_time": format_ts(end_ts),
            "amount_out": amount_out,
            "amount_back": amount_back,
            "accounts": accounts,
            "txn_ids": txn_ids,
            "windows": windows,
            "entities": self._entities(accounts, windows[0] if windows else None),
            "transfers": self._transfers_for(windows, txn_ids, accounts),
        }

    def _residue_file(self, case_id: str) -> dict:
        rows = self.graph_db.read(CASE_GRAPH, RESIDUE_LEAD, {"id": case_id}).result_set
        if not rows:
            return {"lead": case_id, "error": "No residue case with that id"}
        _, kind, account, window, focal, parts, amount, part_sum, currency, ts = rows[0]
        txn_ids = [int(focal)]
        if parts:
            txn_ids.extend(int(txn_id) for txn_id in str(parts).split(",") if txn_id)
        transfers = self._transfers_for([window], txn_ids, [account])
        accounts = [account]
        for transfer in transfers:
            for name in (transfer["src"], transfer["dst"]):
                if name not in accounts:
                    accounts.append(name)
        return {
            "lead": case_id,
            "kind": kind,
            "open": True,
            "account": account,
            "accounts": accounts,
            "txn_ids": txn_ids,
            "windows": [window],
            "amount": amount,
            "part_sum": part_sum,
            "currency": currency,
            "time": format_ts(int(ts)) if ts is not None else None,
            "entities": self._entities(accounts, window),
            "transfers": transfers,
        }

    def _review_file(self, lead: str) -> dict:
        rows = self.graph_db.read(REVIEW_GRAPH, READ_REVIEW, {"id": lead}).result_set
        if not rows:
            return {"lead": lead, "error": "No counterparty lead with that id"}
        _, kind, account, counterparty, raw_txns, window, ts = rows[0]
        txn_ids = _txn_id_list(raw_txns)
        accounts = [account, counterparty]
        windows = [window] if window else []
        for item in self.list_windows()["windows"]:
            if item["graph"] not in windows:
                windows.append(item["graph"])
        transfers = self._transfers_for(windows, txn_ids, accounts)
        return {
            "lead": lead,
            "kind": kind or "counterparty",
            "source": "review",
            "account": account,
            "counterparty": counterparty,
            "accounts": accounts,
            "txn_ids": txn_ids,
            "windows": [window] if window else [],
            "time": format_ts(int(ts)) if ts else None,
            "entities": self._entities(accounts, window or (windows[0] if windows else None)),
            "transfers": transfers,
        }

    def _transfers_for(self, windows: list[str], txn_ids: list[int], accounts: list[str]) -> list[dict]:
        wanted = set(txn_ids)
        found = []
        seen = set()
        for graph in windows:
            self._check_graph(graph)
            for account in accounts:
                rows = _rows(
                    self.graph_db.read(
                        graph, ACCOUNT_EDGES, {"id": account, "limit": MAX_ROWS}
                    ),
                    MAX_ROWS,
                )["rows"]
                for row in rows:
                    if row["txn_id"] not in wanted or row["txn_id"] in seen:
                        continue
                    seen.add(row["txn_id"])
                    row["window"] = graph
                    found.append(row)
            if seen == wanted:
                break
        found.sort(key=lambda row: (row.get("ts") or 0, row["txn_id"]))
        return found

    def _entities(self, accounts: list[str], window: str | None) -> dict:
        return {account: self._entity(account, window) for account in accounts}

    def _windows_covering(self, start_ts: int, end_ts: int) -> list[str]:
        names = []
        for item in self.list_windows()["windows"]:
            start = parse_ts(item["start"])
            end = parse_ts(item["end"])
            if start_ts < end and end_ts >= start:
                names.append(item["graph"])
        return names

    def _entity(self, account_id: str, window: str | None) -> dict:
        if not window:
            return {"entity_id": None, "entity_name": None, "accounts": 0, "other_accounts": []}
        self._check_graph(window)
        found = self.graph_db.read(window, ENTITY, {"id": account_id}).result_set
        if not found or not found[0][0]:
            return {"entity_id": None, "entity_name": None, "accounts": 0, "other_accounts": []}
        entity_id, entity_name = found[0]
        others = [
            row[0]
            for row in self.graph_db.read(
                window, ENTITY_ACCOUNTS, {"entity": entity_id, "id": account_id, "limit": ENTITY_LIMIT}
            ).result_set
        ]
        count = self.graph_db.read(window, ENTITY_COUNT, {"entity": entity_id}).result_set
        return {
            "entity_id": entity_id,
            "entity_name": entity_name,
            "accounts": int(count[0][0]) if count else 0,
            "other_accounts": others,
        }

    def _adjacent(self, window: str) -> list[str]:
        names = [item["graph"] for item in self.list_windows()["windows"]]
        if window not in names:
            raise ValueError(f"Unknown window {window}")
        index = names.index(window)
        return [names[i] for i in (index - 1, index + 1) if 0 <= i < len(names)]

    def _check_graph(self, graph: str) -> None:
        known = (LOOP_GRAPH, FLAG_GRAPH, CASE_GRAPH, DECISION_GRAPH, REVIEW_GRAPH)
        if graph not in known and not graph.startswith(f"{self.windows.prefix}:"):
            raise ValueError(
                "Graph is not a window, the loop graph, the case graph, the flag graph, "
                "the decision graph, or the review graph"
            )
