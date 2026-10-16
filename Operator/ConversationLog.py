"""Operator and agent turns, stored outside the transaction windows.

The words and the shorthand tool calls live in a Redis stream, one stream per
session. Falkor keeps a session node and a pointer for each subject the
conversation touched: a loop, a residue, a counterparty review, an account,
an entity, or a payment.
"""

import time

from adapters.graphdb import GraphDB
from adapters.staticdb import Redis

SESSION_GRAPH = "aml:sessions"
SESSION_INDEXES = [
    "CREATE INDEX FOR (s:Session) ON (s.id)",
    "CREATE INDEX FOR (p:Subject) ON (p.key)",
]

TOUCH_SESSION = """
MERGE (s:Session {id: $id})
SET s.updated_ts = $ts
"""

LINK_SUBJECTS = """
MATCH (s:Session {id: $id})
UNWIND $keys AS key
MERGE (p:Subject {key: key})
MERGE (s)-[:ABOUT]->(p)
"""

SESSIONS_FOR = """
MATCH (p:Subject {key: $key})<-[:ABOUT]-(s:Session)
RETURN s.id AS id
ORDER BY s.updated_ts DESC
LIMIT $limit
"""

SESSION_SUBJECTS = """
MATCH (s:Session {id: $id})-[:ABOUT]->(p:Subject)
RETURN p.key AS key
"""

SET_SHEET = """
MATCH (s:Session {id: $id})
SET s.sheet = $sheet, s.updated_ts = $ts
RETURN s.id AS id
"""

DETACH_SESSION = """
MATCH (s:Session {id: $id})
DETACH DELETE s
"""

DROP_ORPHAN = """
MATCH (p:Subject {key: $key})
WHERE NOT (p)<-[:ABOUT]-()
DELETE p
"""


class ConversationLog:
    def __init__(self, store: Redis, graph_db: GraphDB):
        self.redis = store.client
        self.graph_db = graph_db
        self._ready = False

    def append(
        self,
        session_id: str,
        role: str,
        text: str,
        calls: list[str] | None = None,
        txn_ids: list[int] | None = None,
        accounts: list[str] | None = None,
    ) -> str:
        """Append one turn and point the session at every subject it names."""
        calls = [call for call in (calls or []) if call]
        txn_ids = [int(txn_id) for txn_id in (txn_ids or [])]
        accounts = [account for account in (accounts or []) if account]
        keys = _subject_keys(calls, txn_ids, accounts)
        turn_id = self.redis.xadd(
            _stream(session_id),
            {
                "role": role,
                "text": text or "",
                "calls": " ".join(calls),
                "txn_ids": ",".join(str(txn_id) for txn_id in txn_ids),
                "accounts": ",".join(accounts),
                "ts": str(int(time.time())),
            },
        )
        self._ensure()
        now = int(time.time())
        self.graph_db.query(SESSION_GRAPH, TOUCH_SESSION, {"id": session_id, "ts": now})
        if keys:
            self.graph_db.query(
                SESSION_GRAPH, LINK_SUBJECTS, {"id": session_id, "keys": sorted(keys)}
            )
        return _text(turn_id)

    def read(self, session_id: str) -> list[dict]:
        """The turns of one session, oldest first."""
        rows = []
        for entry_id, fields in self.redis.xrange(_stream(session_id)):
            item = {_text(key): _text(value) for key, value in fields.items()}
            item["id"] = _text(entry_id)
            item["calls"] = [call for call in item.get("calls", "").split(" ") if call]
            item["txn_ids"] = [int(txn_id) for txn_id in item.get("txn_ids", "").split(",") if txn_id]
            item["accounts"] = [account for account in item.get("accounts", "").split(",") if account]
            rows.append(item)
        return rows

    def sessions_for(self, kind: str, subject_id: str, limit: int = 20) -> list[str]:
        """Session ids whose conversation touched this subject."""
        self._ensure()
        rows = self.graph_db.read(
            SESSION_GRAPH,
            SESSIONS_FOR,
            {"key": f"{kind}:{subject_id}", "limit": max(1, min(limit, 50))},
        ).result_set
        return [_text(row[0]) for row in rows]

    def set_sheet(self, session_id: str, path: str) -> bool:
        """Record the workbook path on the session. The narrative stays in the stream."""
        self._ensure()
        rows = self.graph_db.query(
            SESSION_GRAPH,
            SET_SHEET,
            {"id": session_id, "sheet": path, "ts": int(time.time())},
        ).result_set
        return bool(rows)

    def delete(self, session_id: str) -> None:
        self.redis.delete(_stream(session_id))
        self._ensure()
        keys = [
            _text(row[0])
            for row in self.graph_db.read(
                SESSION_GRAPH, SESSION_SUBJECTS, {"id": session_id}
            ).result_set
        ]
        self.graph_db.query(SESSION_GRAPH, DETACH_SESSION, {"id": session_id})
        for key in keys:
            self.graph_db.query(SESSION_GRAPH, DROP_ORPHAN, {"key": key})

    def _ensure(self) -> None:
        if self._ready:
            return
        self.graph_db.ensure_indexes(SESSION_GRAPH, SESSION_INDEXES)
        self._ready = True


def shorthand(name: str, args: dict | None = None) -> str:
    """The tool and the ids that identify the call. Defaults and payloads stay out."""
    args = args or {}
    if name == "list_leads":
        kind = str(args.get("kind") or "all")
        return "leads" if kind == "all" else f"leads:{kind}"
    if name == "list_windows":
        return "windows"
    if name == "query_cypher":
        return "cypher"
    if name == "case_file":
        return f"cf:{args.get('lead', '')}"
    if name == "summarize":
        if args.get("account_id"):
            return f"sum:a:{args['account_id']}"
        if args.get("entity_id"):
            return f"sum:e:{args['entity_id']}"
        return "sum"
    if name == "walk_transaction":
        return f"walk:{args.get('txn_id')}:{args.get('depth', 3)}"
    if name == "explain_transactions":
        return "ex:" + "+".join(str(txn_id) for txn_id in _id_list(args.get("txn_ids")))
    if name == "one_hop_across_windows":
        account = args.get("account_id") or ""
        date = _window_date(str(args.get("window") or ""))
        return f"hop:{account}:{date}" if date else f"hop:{account}"
    if name == "open_workbook":
        return f"sheet:{args.get('lead', '')}"
    if name == "record_decision":
        verb = "keep" if args.get("decision") == "keep" else "dismiss"
        return f"{verb}:{args.get('lead', '')}"
    if name == "decision_for_lead":
        return f"dec:{args.get('lead', '')}"
    if name == "investigate_lead":
        return f"inv:{args.get('lead', '')}"
    code = {
        "account_transfers": "xfer",
        "account_activity": "act",
        "loops_for_account": "loops",
        "cases_for_account": "cases",
        "flags_for_account": "flags",
        "open_counterparty_leads": "rev",
    }.get(name)
    if code:
        return f"{code}:{args.get('account_id', '')}"
    return name


def calls_from_messages(messages: list[dict]) -> list[str]:
    found = []
    for message in messages:
        for call in message.get("tool_calls") or []:
            found.append(shorthand(call.get("name") or "", call.get("args") or {}))
    return found


def cited_from_messages(messages: list[dict]) -> tuple[list[int], list[str]]:
    txn_ids: list[int] = []
    accounts: list[str] = []
    for message in messages:
        for call in message.get("tool_calls") or []:
            args = call.get("args") or {}
            txn_ids.extend(_id_list(args.get("txn_ids")))
            if args.get("txn_id") not in (None, ""):
                txn_ids.extend(_id_list([args.get("txn_id")]))
            for key in ("account_id", "account"):
                if args.get(key):
                    accounts.append(str(args[key]))
            for account in args.get("accounts") or []:
                accounts.append(str(account))
    return _unique_ints(txn_ids), _unique(accounts)


def _subject_keys(calls: list[str], txn_ids: list[int], accounts: list[str]) -> set[str]:
    keys = {f"txn:{txn_id}" for txn_id in txn_ids}
    keys.update(f"account:{account}" for account in accounts)
    for call in calls:
        keys.update(_subjects_in_call(call))
    keys.discard(":")
    return {key for key in keys if _valid_key(key)}


def _subjects_in_call(call: str) -> set[str]:
    code, _, payload = call.partition(":")
    if code == "sum":
        kind, _, ident = payload.partition(":")
        if kind == "a" and ident:
            return {f"account:{ident}"}
        if kind == "e" and ident:
            return {f"entity:{ident}"}
        return set()
    if code == "walk" and payload:
        txn_id, _, _depth = payload.rpartition(":")
        return {f"txn:{txn_id}"} if txn_id else set()
    if code == "ex" and payload:
        return {f"txn:{txn_id}" for txn_id in payload.split("+") if txn_id}
    if code == "hop" and payload:
        account, _, _date = payload.rpartition(":")
        if account and _date.isdigit() and len(_date) == 8:
            return {f"account:{account}"}
        return {f"account:{payload}"}
    if code == "rev" and payload:
        return {f"account:{payload}"}
    if code in {"xfer", "act", "loops", "cases", "flags"} and payload:
        return {f"account:{payload}"}
    if code in {"cf", "dec", "keep", "dismiss", "inv", "sheet"} and payload:
        return {_classify(payload)}
    return set()


def _classify(payload: str) -> str:
    if payload.startswith("review:"):
        return payload
    if payload.startswith("pooled:") or payload.startswith("split:"):
        return f"residue:{payload}"
    if "," in payload and all(part.isdigit() for part in payload.split(",")):
        return f"loop:{payload}"
    if payload.isdigit():
        return f"txn:{payload}"
    bank, sep, number = payload.partition(":")
    if sep and bank.isdigit() and number and ":" not in number:
        return f"account:{payload}"
    return f"entity:{payload}"


def _valid_key(key: str) -> bool:
    kind, sep, ident = key.partition(":")
    return bool(sep and ident and kind in {"loop", "residue", "review", "account", "entity", "txn"})


def _window_date(window: str) -> str:
    marker = "aml:txn:"
    if not window.startswith(marker):
        return ""
    return window[len(marker) : len(marker) + 8]


def _id_list(raw) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, str):
        values = [part.strip() for part in raw.replace("+", ",").split(",") if part.strip()]
    else:
        values = list(raw)
    found = []
    for value in values:
        try:
            found.append(int(value))
        except (TypeError, ValueError):
            continue
    return found


def _unique(values: list[str]) -> list[str]:
    seen = []
    for value in values:
        if value and value not in seen:
            seen.append(value)
    return seen


def _unique_ints(values: list[int]) -> list[int]:
    seen = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return seen


def _stream(session_id: str) -> str:
    return f"aml:log:{session_id}"


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
