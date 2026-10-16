"""Run one operator conversation through the LangChain agent.

The chat model is simulated. The MCP tools and the conversation log are the
ones the operator session uses. No API key is required.

Usage:
    .venv/bin/python scripts/simulate_conversation.py
"""

import asyncio
import json
import sys
import warnings
from pathlib import Path
from uuid import uuid4

from langchain_core._api import LangChainBetaWarning

warnings.filterwarnings("ignore", category=LangChainBetaWarning)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from adapters.staticdb import Redis  # noqa: E402
from langchain.mcp import MCPAdapter  # noqa: E402

from Operator.ConversationLog import ConversationLog  # noqa: E402
from Operator.session import _unwrap, investigate, open_agent, server_client  # noqa: E402
from Operator.simulated import SimulatedOperator  # noqa: E402

OUT = PROJECT_ROOT / "presentation" / "operator-pipeline" / "simulated-conversation.json"
CLEAR_DECISION = """
MATCH (d:Decision {lead: $lead})
DELETE d
"""


async def main() -> None:
    model = SimulatedOperator()
    log = ConversationLog(Redis(), FalkorGraphDB())
    session_id = "sim-" + uuid4().hex
    transcript = []
    async with MCPAdapter(server_client()) as adapter:
        tools = await adapter.list_tools()
        agent = open_agent(tools, model)
        opened = await _turn(
            tools, model, log, session_id, transcript, "List the leads I can open.", "", agent
        )
        lead = _first_loop(opened["messages"])
        if not lead:
            raise RuntimeError("list_leads did not return a loop")
        case_turn = await _turn(
            tools,
            model,
            log,
            session_id,
            transcript,
            f"Open lead {lead} and explain how the payments connect.",
            lead,
            agent,
        )
        case = _payload(case_turn["messages"], "case_file")
        account = (case.get("accounts") or [""])[0]
        txn_id = (case.get("txn_ids") or [0])[0]
        if not account or not txn_id:
            raise RuntimeError("case_file did not name an account and a payment")
        await _turn(
            tools,
            model,
            log,
            session_id,
            transcript,
            f"Summarize account {account}.",
            lead,
            agent,
        )
        await _turn(
            tools,
            model,
            log,
            session_id,
            transcript,
            f"Walk down from transaction {txn_id} two steps.",
            lead,
            agent,
        )
        await _turn(
            tools,
            model,
            log,
            session_id,
            transcript,
            f"Keep lead {lead}. Payments on this lead should be kept.",
            lead,
            agent,
        )
    stored = log.read(session_id)
    found = log.sessions_for("loop", lead)
    _clear_decision(lead)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps({"session_id": session_id, "lead": lead, "turns": transcript, "log": stored}, indent=2)
        + "\n"
    )
    print("session", session_id)
    for turn in transcript:
        print()
        print("operator>", turn["operator"])
        print("agent>", turn["agent"])
        print("calls>", " ".join(turn["calls"]) or "(none)")
    print()
    print(f"log turns={len(stored)} found_by_loop={session_id in found}")


async def _turn(tools, model, log, session_id, transcript, question: str, lead: str, agent) -> dict:
    answer = await investigate(
        tools,
        lead,
        question,
        log=log,
        session_id=session_id,
        model=model,
        agent=agent,
    )
    transcript.append(
        {
            "operator": question,
            "agent": answer.get("answer") or "",
            "calls": _calls(answer["messages"]),
            "decision": answer.get("decision"),
        }
    )
    return answer


def _calls(messages: list[dict]) -> list[str]:
    from Operator.ConversationLog import calls_from_messages

    return calls_from_messages(messages)


def _payload(messages: list[dict], name: str) -> dict:
    for message in messages:
        if message.get("type") == "tool" and message.get("name") == name:
            payload = _unwrap(message.get("content"))
            return payload if isinstance(payload, dict) else {}
    return {}


def _first_loop(messages: list[dict]) -> str:
    payload = _payload(messages, "list_leads")
    for row in payload.get("leads") or []:
        if row.get("source") == "loop" and row.get("lead"):
            return str(row["lead"])
    return ""


def _clear_decision(lead: str) -> None:
    FalkorGraphDB().query("aml:decisions", CLEAR_DECISION, {"lead": lead})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
