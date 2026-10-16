"""A stand-in chat model for one operator conversation.

It follows the same starts as the operator prompt: leads, a case file, a
summary, a walk, then keep or dismiss. Each step is a LangChain tool call.
The tools themselves are the real MCP tools; this model only chooses them
from the operator's question and the results already in the turn.
"""

import re
from uuid import uuid4

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from Operator.session import _unwrap


class SimulatedOperator(BaseChatModel):
    """Chooses the next tool, or answers from the tools this turn already called."""

    @property
    def _llm_type(self) -> str:
        return "simulated-operator"

    def bind_tools(self, tools, *, tool_choice=None, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=_reply(messages))])


def _reply(messages) -> AIMessage:
    question = _question(messages)
    done = _tool_results(messages)
    intent = _intent(question)
    if intent == "leads" and "list_leads" not in done:
        return _call("list_leads", {"kind": "all", "limit": 3})
    if intent == "open" and "case_file" not in done:
        return _call("case_file", {"lead": _lead(question)})
    if intent == "open" and _wants_connection(question) and "explain_transactions" not in done:
        case = done.get("case_file") or {}
        if case.get("txn_ids") and not case.get("error"):
            return _call("explain_transactions", {"txn_ids": case["txn_ids"]})
    if intent == "summarize" and "summarize" not in done:
        account = _account(question)
        entity = _entity(question)
        if account:
            return _call("summarize", {"account_id": account})
        if entity:
            return _call("summarize", {"entity_id": entity})
    if intent == "walk" and "walk_transaction" not in done:
        return _call(
            "walk_transaction",
            {"txn_id": _txn(question), "depth": 2 if re.search(r"\b(two|2)\b", question) else 3},
        )
    if intent == "decide" and "case_file" not in done:
        return _call("case_file", {"lead": _lead(question)})
    if intent == "decide" and "record_decision" not in done:
        case = done.get("case_file") or {}
        if case.get("txn_ids") and case.get("accounts") and not case.get("error"):
            decision = "dismiss" if "dismiss" in question.lower() else "keep"
            accounts = [] if decision == "dismiss" else list(case["accounts"])
            return _call(
                "record_decision",
                {
                    "lead": case.get("lead") or _lead(question),
                    "decision": decision,
                    "accounts": accounts,
                    "txn_ids": list(case["txn_ids"]),
                    "reason": _reason(case["txn_ids"], decision),
                },
            )
    return AIMessage(content=_answer(intent, question, done))


def _answer(intent: str, question: str, done: dict) -> str:
    if intent == "leads":
        return _leads_answer(done.get("list_leads") or {})
    if intent == "open":
        return _open_answer(done.get("case_file") or {}, done.get("explain_transactions") or {})
    if intent == "summarize":
        return _summary_answer(done.get("summarize") or {})
    if intent == "walk":
        return _walk_answer(done.get("walk_transaction") or {})
    if intent == "decide":
        return _decision_answer(done.get("record_decision") or {}, done.get("case_file") or {})
    return "I need a lead, an account, or a transaction id to continue."


def _leads_answer(payload: dict) -> str:
    if payload.get("error"):
        return str(payload["error"])
    leads = payload.get("leads") or []
    loops = [row["lead"] for row in leads if row.get("source") == "loop"]
    residues = [row["lead"] for row in leads if row.get("source") == "residue"]
    parts = [f"This page has {len(leads)} leads."]
    if loops:
        parts.append("Loops: " + "; ".join(loops) + ".")
    if residues:
        parts.append("Residues: " + "; ".join(residues) + ".")
    if payload.get("truncated"):
        parts.append("Further leads are on the next page.")
    return " ".join(parts)


def _open_answer(case: dict, explained: dict) -> str:
    if case.get("error"):
        return str(case["error"])
    if not case:
        return "The case file did not come back."
    accounts = ", ".join(case.get("accounts") or [])
    txn_ids = ", ".join(str(txn_id) for txn_id in case.get("txn_ids") or [])
    text = (
        f"Lead {case.get('lead')} is a {case.get('kind')} with payments {txn_ids} "
        f"between {accounts}."
    )
    if explained.get("error"):
        return text + " " + str(explained["error"])
    if explained.get("graphs") is not None:
        count = len(explained["graphs"])
        piece = "1 connected graph" if count == 1 else f"{count} connected graphs"
        text += f" Those payments form {piece}."
    return text


def _summary_answer(payload: dict) -> str:
    if payload.get("error"):
        return str(payload["error"])
    if not payload:
        return "The summary did not come back."
    if payload.get("scope") == "entity":
        return (
            f"Entity {payload.get('entity_id')} links {payload.get('account_count')} accounts. "
            f"{payload.get('note') or ''}"
        ).strip()
    entity = payload.get("entity") or {}
    name = entity.get("entity_name") or entity.get("name") or ""
    link = entity.get("entity_id") or entity.get("id") or ""
    party = f" Entity link {link} {name}.".rstrip()
    count = len(payload.get("transfers") or [])
    complete = "The account read is complete." if payload.get("complete") else "The account read stopped early."
    return f"Account {payload.get('account')} has {count} payments.{party} {complete}"


def _walk_answer(payload: dict) -> str:
    if payload.get("error"):
        return str(payload["error"])
    if not payload:
        return "The walk did not come back."
    steps = payload.get("steps") or []
    root = payload.get("root") or {}
    receiver = root.get("dst") or root.get("to") or ""
    stopped = "The walk stopped early." if payload.get("truncated") else "The walk finished inside its limits."
    return (
        f"Payment {payload.get('txn_id')} was received by {receiver}. "
        f"The walk has {len(steps)} steps. {stopped}"
    )


def _decision_answer(stored: dict, case: dict) -> str:
    if stored.get("error"):
        return str(stored["error"])
    if not stored:
        return "The decision was not stored."
    lead = stored.get("lead") or case.get("lead")
    return f"Recorded {stored.get('decision')} for lead {lead}."


def _reason(txn_ids: list, decision: str) -> str:
    cited = " and ".join(str(txn_id) for txn_id in txn_ids)
    if decision == "dismiss":
        return f"Payments {cited} read as an ordinary pair."
    return f"Payments {cited} return between the same accounts."


def _intent(question: str) -> str:
    text = question.lower()
    if "keep" in text or "dismiss" in text:
        return "decide"
    if "walk" in text:
        return "walk"
    if "summar" in text:
        return "summarize"
    if "explain" in text or "connect" in text or "open lead" in text:
        return "open"
    if "lead" in text:
        return "leads"
    return ""


def _wants_connection(question: str) -> bool:
    text = question.lower()
    return "explain" in text or "connect" in text


def _lead(question: str) -> str:
    residue = re.search(r"(?:pooled|split):\S+", question)
    if residue:
        return residue.group(0)
    loop = re.search(r"\d+(?:,\d+)+", question)
    return loop.group(0) if loop else ""


def _account(question: str) -> str:
    found = re.search(r"\d+:[0-9A-Za-z]+", question)
    return found.group(0) if found else ""


def _entity(question: str) -> str:
    found = re.search(r"entity\s+([0-9A-Za-z]+)", question, re.IGNORECASE)
    return found.group(1) if found else ""


def _txn(question: str) -> int:
    found = re.search(r"transaction\s+(\d+)", question, re.IGNORECASE)
    return int(found.group(1)) if found else 0


def _question(messages) -> str:
    for message in reversed(messages):
        if getattr(message, "type", None) != "human":
            continue
        content = message.content
        if isinstance(content, str):
            return content
        return str(content)
    return ""


def _tool_results(messages) -> dict:
    found = {}
    for message in messages:
        if getattr(message, "type", None) != "tool":
            continue
        found[getattr(message, "name", "") or ""] = _unwrap(message.content)
    return found


def _call(name: str, args: dict) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[{"name": name, "args": args, "id": uuid4().hex, "type": "tool_call"}],
    )
