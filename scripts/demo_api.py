"""One empty chat on the slice, for the Verico workspace.

The thread lives in this process only. Nothing is listed or restored.
A reply is streamed as server-sent events. The graph is a short walk from
the payment the agent is discussing. The case book is offered only after
this chat writes one.

    .venv/bin/python scripts/demo_api.py
"""

import asyncio
import json
import os
import re
import sys
import warnings
from pathlib import Path
from uuid import uuid4

from langchain_core._api import LangChainBetaWarning

warnings.filterwarnings("ignore", category=LangChainBetaWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import FileResponse, StreamingResponse  # noqa: E402
from pydantic import BaseModel  # noqa: E402
import uvicorn  # noqa: E402

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from adapters.staticdb import Redis  # noqa: E402
from langchain.mcp import MCPAdapter  # noqa: E402

from GraphRead.reader import LOOP_GRAPH, GraphReader  # noqa: E402
from GraphRead.reviews import REVIEW_GRAPH  # noqa: E402
from GraphIngest.WindowFlags import FLAG_GRAPH  # noqa: E402
from Detection.ResidueCases import CASE_GRAPH  # noqa: E402
from Operator.ConversationLog import ConversationLog, calls_from_messages  # noqa: E402
from Operator.session import REQUIRED, investigate, open_agent, server_client  # noqa: E402
from Operator.workbook import CASES  # noqa: E402

PARTY = "MATCH (a:Account {id: $id}) RETURN a.entity_name AS entity_name LIMIT 1"
PAYMENT_CAP = 24

def _origins() -> list[str]:
    origins = ["http://localhost:3000", "http://127.0.0.1:3000"]
    extra = os.environ.get("VERICO_ORIGIN", "")
    origins.extend(item.strip().rstrip("/") for item in extra.split(",") if item.strip())
    return origins


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins(),
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class MessageIn(BaseModel):
    message: str
    session_id: str = ""


@app.on_event("startup")
async def _open_tools() -> None:
    app.state.adapter = MCPAdapter(server_client())
    await app.state.adapter.__aenter__()
    tools = await app.state.adapter.list_tools()
    missing = [name for name in REQUIRED if name not in {tool.name for tool in tools}]
    if missing:
        raise RuntimeError("MCP server is missing tools: " + ", ".join(missing))
    app.state.tools = tools
    app.state.sessions = {}


@app.on_event("shutdown")
async def _close_tools() -> None:
    adapter = getattr(app.state, "adapter", None)
    if adapter is not None:
        await adapter.__aexit__(None, None, None)


@app.post("/demo/messages")
async def messages(body: MessageIn):
    text = body.message.strip()
    if not text:
        return _json_error("Write a question to start.")
    session_id = body.session_id or uuid4().hex
    holder = app.state.sessions.get(session_id)
    if holder is None:
        log = ConversationLog(Redis(), FalkorGraphDB())
        holder = {
            "id": session_id,
            "log": log,
            "agent": open_agent(app.state.tools, log=log, session_id=session_id),
            "lock": asyncio.Lock(),
            "focal": None,
            "book": False,
        }
        app.state.sessions[session_id] = holder

    async def events():
        yield _event({"type": "session", "session_id": session_id})
        queue: asyncio.Queue = asyncio.Queue()

        async def emit(piece: str) -> None:
            await queue.put(("token", piece))

        async def run() -> None:
            try:
                async with holder["lock"]:
                    answer = await investigate(
                        app.state.tools,
                        "",
                        text,
                        log=holder["log"],
                        session_id=session_id,
                        agent=holder["agent"],
                        stream=True,
                        emit=emit,
                    )
                graph, focal = _graph(holder, answer.get("messages") or [])
                _remember_stage(holder, answer.get("messages") or [])
                book = _book(holder, answer.get("messages") or [])
                spoken = _mark_section(
                    answer.get("answer") or "",
                    answer.get("messages") or [],
                    graph,
                    holder,
                )
                prompts = _shape_prompts(answer.get("prompts") or [], text, holder)
                await queue.put(
                    (
                        "done",
                        {
                            "type": "done",
                            "session_id": session_id,
                            "answer": spoken,
                            "graph": graph,
                            "book": book,
                            "prompts": prompts,
                        },
                    )
                )
            except Exception as err:
                await queue.put(("error", {"type": "error", "error": str(err)}))
            finally:
                await queue.put(None)

        task = asyncio.create_task(run())
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                kind, payload = item
                if kind == "token":
                    yield _event({"type": "token", "text": payload})
                else:
                    yield _event(payload)
        finally:
            await task

    return StreamingResponse(events(), media_type="text/event-stream")


@app.get("/demo/book/{session_id}")
def book(session_id: str):
    path = CASES / f"{_safe(session_id)}.xlsx"
    holder = app.state.sessions.get(session_id)
    if holder is None or not holder.get("book") or not path.is_file():
        return _json_error("A case book is written when you ask for one.")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="verico-case-book.xlsx",
    )


def _graph(holder: dict, messages: list[dict]) -> tuple[dict | None, str | None]:
    payments, names, subject, focal = _tool_payments(messages)
    if not payments:
        graph = holder.get("graph")
        if graph:
            _apply_marks(holder, graph)
        return graph, holder.get("focal")
    if subject is None:
        subject = _busiest(payments)
    kept = [row for row in payments if subject in (row["src"], row["dst"])] or payments
    kept = kept[:PAYMENT_CAP]
    accounts = {row["src"] for row in kept} | {row["dst"] for row in kept}
    window = next((row["window"] for row in kept if row.get("window")), None)
    missing = accounts.difference(names)
    if missing and window:
        for account, name in _parties(GraphReader(holder["log"].graph_db), window, missing).items():
            names[account] = name
    focal_id = str(focal) if focal and any(row["txn_id"] == focal for row in kept) else str(kept[0]["txn_id"])
    nodes = [
        {
            "id": account,
            "name": names.get(account) or account,
            "role": "subject" if account == subject else "party",
        }
        for account in sorted(accounts, key=lambda account: (account != subject, names.get(account) or account))
    ]
    edges = [
        {
            "id": str(row["txn_id"]),
            "from": row["src"],
            "to": row["dst"],
            "txn_id": row["txn_id"],
            "amount": row["amount"],
            "currency": row["currency"],
            "format": row["format"],
            "time": row["time"],
            "src_name": names.get(row["src"]) or row["src"],
            "dst_name": names.get(row["dst"]) or row["dst"],
            "focal": str(row["txn_id"]) == focal_id,
        }
        for row in sorted(kept, key=lambda row: (row["time"], row["txn_id"]))
    ]
    graph = {"focal": focal_id, "nodes": nodes, "edges": edges}
    _apply_marks(holder, graph)
    holder["graph"] = graph
    holder["focal"] = focal_id
    return graph, focal_id


_FLAGGED = """
MATCH (f:Flag)
WHERE f.account IN $ids
RETURN DISTINCT f.account AS account
"""

_IN_LOOP = """
MATCH (a:Account)-[:IN_LOOP]->(:Loop)
WHERE a.id IN $ids
RETURN DISTINCT a.id AS account
"""

_IN_CASE = """
MATCH (c:Case)
WHERE c.account IN $ids
RETURN DISTINCT c.account AS account
"""

_IN_REVIEW = """
MATCH (r:Review)
WHERE r.account IN $ids OR r.counterparty IN $ids
RETURN r.account AS account, r.counterparty AS counterparty
"""


_FLAG_DETAIL = """
MATCH (f:Flag)
WHERE f.account IN $ids
RETURN f.account AS account, f.rule AS rule
"""

_MARK_WORDS = {
    "fan_in": "many accounts sent money to {name}",
    "fan_out": "{name} sent money to many accounts",
    "gather": "a payment that left {name} matched the payments that arrived",
}


def _mark_section(answer: str, messages: list[dict], graph: dict | None, holder: dict) -> str:
    """The mark section is taken from the stored flags, not from a guess."""
    tools = {message.get("name") for message in messages if message.get("type") == "tool"}
    payments = tools & {"case_file", "explain_transactions", "walk_transaction"}
    accounts = tools & {"summarize", "account_activity", "account_transfers", "open_counterparty_leads"}
    if not graph or not (payments or accounts):
        return answer
    heading = (
        "## Should we be careful of this account"
        if accounts and not payments
        else "## Is there any suspicious thing"
    )
    cleaned = re.sub(
        r"\n+## (?:Is there any suspicious thing|Should we be careful of this account)\n.*?(?=\n## |\Z)",
        "",
        answer,
        flags=re.S,
    ).rstrip()
    return f"{cleaned}\n\n{heading}\n\n{_mark_words(holder, graph)}"


def _mark_words(holder: dict, graph: dict) -> str:
    nodes = graph.get("nodes") or []
    names = {node["id"]: node.get("name") or node["id"] for node in nodes}
    rows = _rows(
        GraphReader(holder["log"].graph_db),
        FLAG_GRAPH,
        _FLAG_DETAIL,
        list(names),
    )
    said = []
    lines = []
    for row in rows:
        if not row or not row[0]:
            continue
        account, rule = str(row[0]), str(row[1] or "")
        if (account, rule) in said or rule not in _MARK_WORDS:
            continue
        said.append((account, rule))
        sentence = _MARK_WORDS[rule].format(name=names.get(account, account))
        sentence = sentence[0].upper() + sentence[1:]
        lines.append(sentence + ". That mark is not a lead.")
    if not lines:
        return "There is no mark on these accounts."
    return " ".join(lines)


def _apply_marks(holder: dict, graph: dict) -> None:
    accounts = {node["id"] for node in graph.get("nodes") or []}
    marks = _marks(GraphReader(holder["log"].graph_db), accounts)
    for node in graph["nodes"]:
        node["mark"] = marks.get(node["id"], "")


def _marks(reader: GraphReader, accounts: set[str]) -> dict[str, str]:
    """Red is a flag already on the account. Brown is a lead the account sits on."""
    if not accounts:
        return {}
    ids = list(accounts)
    flagged = _column(reader, FLAG_GRAPH, _FLAGGED, ids)
    leads = set()
    leads.update(_column(reader, LOOP_GRAPH, _IN_LOOP, ids))
    leads.update(_column(reader, CASE_GRAPH, _IN_CASE, ids))
    for row in _rows(reader, REVIEW_GRAPH, _IN_REVIEW, ids):
        leads.update(str(value) for value in row[:2] if value)
    return {
        account: "flagged" if account in flagged else "lead"
        for account in accounts
        if account in flagged or account in leads
    }


def _column(reader: GraphReader, graph: str, cypher: str, ids: list[str]) -> set[str]:
    return {str(row[0]) for row in _rows(reader, graph, cypher, ids) if row and row[0]}


def _rows(reader: GraphReader, graph: str, cypher: str, ids: list[str]) -> list:
    try:
        return reader.graph_db.read(graph, cypher, {"ids": ids}).result_set or []
    except Exception:
        return []


def _tool_payments(messages: list[dict]) -> tuple[list[dict], dict[str, str], str | None, int | None]:
    payments: dict[int, dict] = {}
    names: dict[str, str] = {}
    subject = None
    focal = None
    for message in messages:
        if message.get("type") != "tool":
            continue
        payload = _load(message.get("content"))
        if not isinstance(payload, dict) or payload.get("error"):
            continue
        kind = message.get("name")
        if kind == "case_file":
            for account, entity in (payload.get("entities") or {}).items():
                if isinstance(entity, dict) and entity.get("entity_name"):
                    names[str(account)] = str(entity["entity_name"])
            accounts = payload.get("accounts") or []
            if accounts and subject is None:
                subject = str(accounts[0])
            if payload.get("focal_txn") not in (None, ""):
                focal = int(payload["focal_txn"])
            elif payload.get("txn_ids") and focal is None:
                focal = int(payload["txn_ids"][0])
            for row in payload.get("transfers") or []:
                _remember(payments, row.get("src"), row.get("dst"), row)
        elif kind in {"summarize", "account_transfers"} and payload.get("account"):
            subject = str(payload["account"])
            entity = payload.get("entity") or {}
            if isinstance(entity, dict) and entity.get("entity_name"):
                names[subject] = str(entity["entity_name"])
            for row in payload.get("transfers") or []:
                src, dst = _ends(subject, row)
                _remember(payments, src, dst, row)
        elif kind == "account_activity" and payload.get("account"):
            subject = str(payload["account"])
            entity = payload.get("entity") or {}
            if isinstance(entity, dict) and entity.get("entity_name"):
                names[subject] = str(entity["entity_name"])
            for window in payload.get("windows") or []:
                for row in window.get("transfers") or []:
                    src, dst = _ends(subject, row)
                    _remember(payments, src, dst, row)
        elif kind == "explain_transactions":
            for graph in payload.get("graphs") or []:
                for row in graph.get("transfers") or []:
                    _remember(payments, row.get("src"), row.get("dst"), row)
        elif kind == "walk_transaction" and isinstance(payload.get("root"), dict):
            root = payload["root"]
            focal = int(root["txn_id"])
            _remember(payments, root.get("src"), root.get("dst"), root)
            for step in payload.get("steps") or []:
                for row in step.get("transfers") or []:
                    _remember(payments, step.get("from_account"), row.get("other"), row)
    return list(payments.values()), names, subject, focal


def _remember(payments: dict[int, dict], src, dst, row: dict) -> None:
    if not src or not dst or src == dst or row.get("txn_id") in (None, ""):
        return
    txn_id = int(row["txn_id"])
    payments[txn_id] = {
        "txn_id": txn_id,
        "src": str(src),
        "dst": str(dst),
        "amount": row.get("amount_paid"),
        "currency": row.get("payment_currency") or "",
        "format": row.get("payment_format") or "",
        "time": row.get("time") or "",
        "window": row.get("window"),
    }


def _ends(account: str, row: dict) -> tuple[str, str]:
    if row.get("src") and row.get("dst"):
        return str(row["src"]), str(row["dst"])
    other = str(row.get("other") or "")
    if row.get("direction") == "out":
        return account, other
    return other, account


def _busiest(payments: list[dict]) -> str:
    counts: dict[str, int] = {}
    for row in payments:
        counts[row["src"]] = counts.get(row["src"], 0) + 1
        counts[row["dst"]] = counts.get(row["dst"], 0) + 1
    return max(counts, key=counts.get)


def _parties(reader: GraphReader, window: str | None, accounts: set[str]) -> dict[str, str]:
    if not window:
        return {}
    names = {}
    for account in accounts:
        try:
            rows = reader.graph_db.read(window, PARTY, {"id": account}).result_set
        except Exception:
            continue
        if rows and rows[0][0]:
            names[account] = str(rows[0][0])
    return names


def _book(holder: dict, messages: list[dict]) -> bool:
    calls = calls_from_messages(messages)
    path = CASES / f"{_safe(holder['id'])}.xlsx"
    if any(call.startswith("sheet:") for call in calls) and path.is_file():
        holder["book"] = True
    return bool(holder.get("book"))


def _load(content):
    if isinstance(content, list):
        text = "".join(
            block.get("text", "") if isinstance(block, dict) else str(block) for block in content
        )
        return _load(text) if text else None
    if isinstance(content, dict):
        return content
    if isinstance(content, str):
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None
    return None


def _safe(session_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in session_id)


def _event(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


CLOSED_FALLBACK = [
    "Open the first lead",
    "Look at the next lead",
    "Show the payments on the third lead",
]
OPEN_FALLBACK = [
    "Let's look at these accounts",
    "Flag the account and the accounts that dealt with it",
]
_ASKS = re.compile(
    r"^(would you|could you|can you|do you|please (clarify|provide|specify|tell)|which |what |who )",
    re.I,
)


def _remember_stage(holder: dict, messages: list[dict]) -> None:
    calls = calls_from_messages(messages)
    if any(call.startswith("cf:") for call in calls):
        holder["opened"] = True
    if any(call.startswith("keep:") for call in calls):
        holder["kept"] = True


def _shape_prompts(prompts: list[str], question: str, holder: dict) -> list[str]:
    """Keep the officer's next commands in order. Drop questions aimed back at them."""
    opened = bool(holder.get("opened"))
    kept = bool(holder.get("kept"))
    book = bool(holder.get("book"))
    if opened and not kept:
        fixed = ["Keep the lead", "Open the case book"]
    elif opened and not book:
        fixed = ["Open the case book"]
    else:
        fixed = []
    commands = [prompt for prompt in prompts if _command(prompt)]
    if not opened:
        commands = _without_ids(commands)
    fallback = OPEN_FALLBACK if opened else CLOSED_FALLBACK
    if len(commands) < 3 - len(fixed):
        commands = commands + [item for item in fallback if item not in commands]
    return _merge_prompts(fixed, commands, question)[:3]


def _without_ids(commands: list[str]) -> list[str]:
    """A listed lead is the first, second, or a different one. Tool numbers stay out of the buttons."""
    if commands and all(re.search(r"\d", command) for command in commands):
        return ["Open the first lead", "Look at the second lead", "Look at a different lead"]
    return commands


def _command(prompt: str) -> bool:
    text = " ".join(str(prompt).split())
    if not text or text.endswith("?"):
        return False
    if _ASKS.match(text):
        return False
    folded = text.casefold()
    return "clarify" not in folded and "would you like" not in folded


def _merge_prompts(fixed: list[str], extra: list[str], question: str) -> list[str]:
    asked = question.strip().casefold()
    prompts = []
    seen = set()
    for prompt in fixed + extra:
        text = " ".join(str(prompt).split())
        folded = text.casefold()
        if folded and folded != asked and folded not in seen:
            seen.add(folded)
            prompts.append(text[:180])
    return prompts


def _json_error(text: str):
    from fastapi.responses import JSONResponse

    return JSONResponse({"error": text}, status_code=400)


if __name__ == "__main__":
    public = os.environ.get("DEMO_API_URL", "").strip()
    uvicorn.run(app, host="0.0.0.0" if public else "127.0.0.1", port=8787)
