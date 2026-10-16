"""Operator session: check the graph server, then answer one question.

The check and the answer both go through the MCP tools. One agent and one
thread hold the conversation for the session. A finished turn is appended
to the conversation log.
"""

import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from fastmcp.client import Client
from fastmcp.client.transports import StdioTransport
from langchain.agents import create_agent
from langchain.mcp import MCPAdapter
from langgraph.checkpoint.memory import InMemorySaver

from GraphRead.reader import GraphReader
from Operator.ConversationLog import ConversationLog, calls_from_messages, cited_from_messages
from Operator.workbook import refresh_workbook, workbook_tool

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
SERVER = ROOT / "GraphRead" / "server.py"
PYTHON = ROOT / ".venv" / "bin" / "python"
OUT = ROOT / "presentation" / "operator-pipeline"

REQUIRED = (
    "list_leads",
    "summarize",
    "walk_transaction",
    "list_windows",
    "case_file",
    "account_activity",
    "account_transfers",
    "one_hop_across_windows",
    "loops_for_account",
    "cases_for_account",
    "flags_for_account",
    "query_cypher",
    "explain_transactions",
    "decision_for_lead",
    "record_decision",
    "investigate_lead",
    "open_counterparty_leads",
)

SYSTEM = """You assist one compliance officer reviewing payments. You do not watch the payment stream. This is one conversation: a later question refers to the payments already read. "This" and "that lead" mean the payments just discussed. Do not ask the officer to repeat an account, a party, or a payment id.

Write the reply as plain sentences a reviewer can read aloud. No headings, no bold, no backticks, no bullet lists. Speak only in the officer's words. A loop is money that left an account and later came back; you may say loop. Several payments that add up to one payment, or one payment divided into several, must be described that way. Do not say residue, pooled, split, walk, step, depth, window, graph, tool, branch, or chain. Do not say that a search ended, that nothing was found within a depth, or that a payment is outgoing or incoming. Say who sent the money, who received it, the amount, the time, and how it was paid. If no later payment left the account that received the money, say that the money stopped there.

Use the tools to read, then answer in that voice. The tool names below are for you only and must not appear in the reply. Leads are opened with case_file. An account summary uses summarize. Later payments from one payment use walk_transaction. How a set of payments connects uses explain_transactions. When they name one lead, open that lead. Do not also list every other lead unless they ask. When the officer asks what you think, give that reading from the payments already in the conversation. When they say keep or dismiss, call record_decision with the payment ids and accounts already read. Do not ask them to supply those ids. keep means a person should look at the payments. dismiss means the payments read as ordinary. The reason must mention each payment id you record. When they ask for more leads, call list_leads with skip past the ones already shown, name a few in sentences, and say that more exist. When they ask to flag an account, or to open the accounts that dealt with it, call open_counterparty_leads for that account. That writes one lead for each of those accounts, separate from a loop and from payments that add up. Open one of those leads with case_file when they want the payments between the two accounts, and describe those payments in plain sentences. When they ask for a workbook, call open_workbook with the lead already open. If they also ask to keep or dismiss, call record_decision for that lead first, then open_workbook. Do not call investigate_lead.
"""


HF_ROUTER = "https://router.huggingface.co/v1"
HF_MODEL = "deepseek-ai/DeepSeek-V3:deepinfra"


def chat_model():
    """The chat model for this operator session.

    A Hugging Face inference model is an OpenAI-compatible client pointed at
    the HF router. The agent still calls the local MCP tools itself.
    """
    provider, gemini, openai, huggingface = _provider()
    if provider == "gemini":
        if not gemini:
            raise RuntimeError("Set GEMINI_API_KEY or GOOGLE_API_KEY to run the operator session")
        if not os.environ.get("GOOGLE_API_KEY"):
            os.environ["GOOGLE_API_KEY"] = gemini
        return "google_genai:" + os.environ.get("INVESTIGATOR_MODEL", "gemini-2.5-flash")
    if provider == "openai":
        if not openai:
            raise RuntimeError("Set OPENAI_API_KEY to run the operator session")
        return "openai:" + os.environ.get("INVESTIGATOR_MODEL", "gpt-4o-mini")
    if provider == "huggingface":
        if not huggingface:
            raise RuntimeError("Set HF_TOKEN to run the operator session")
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=os.environ.get("INVESTIGATOR_MODEL", HF_MODEL),
            api_key=huggingface,
            base_url=HF_ROUTER,
            use_responses_api=False,
        )
    raise RuntimeError("Set HF_TOKEN, GEMINI_API_KEY, or OPENAI_API_KEY to run the operator session")


def _provider() -> tuple[str, str, str, str]:
    provider = os.environ.get("INVESTIGATOR_PROVIDER", "").strip().lower()
    if provider == "hf":
        provider = "huggingface"
    gemini = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or ""
    openai = os.environ.get("OPENAI_API_KEY") or ""
    huggingface = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN") or ""
    if not provider:
        provider = (
            "gemini"
            if gemini
            else "openai"
            if openai
            else "huggingface"
            if huggingface
            else ""
        )
    return provider, gemini, openai, huggingface


def server_client() -> Client:
    transport = StdioTransport(
        command=str(PYTHON),
        args=[str(SERVER)],
        cwd=str(ROOT),
        keep_alive=False,
    )
    return Client(transport)


async def check_mcp(tools: list, lead: str) -> dict:
    """Call the tools an operator depends on and fail if one does not answer."""
    by_name = {tool.name: tool for tool in tools}
    missing = [name for name in REQUIRED if name not in by_name]
    if missing:
        raise RuntimeError("MCP server is missing tools: " + ", ".join(missing))

    windows = _unwrap(await by_name["list_windows"].ainvoke({}))
    case = _unwrap(await by_name["case_file"].ainvoke({"lead": lead}))
    if case.get("error") or not case.get("transfers"):
        raise RuntimeError("case_file did not return the lead's payments")
    explained = _unwrap(
        await by_name["explain_transactions"].ainvoke({"txn_ids": case["txn_ids"]})
    )
    if explained.get("error") or explained.get("missing") or not explained.get("graphs"):
        raise RuntimeError("explain_transactions did not return a graph of the lead")
    leads = _unwrap(await by_name["list_leads"].ainvoke({"kind": "all", "limit": 3}))
    if not leads.get("leads"):
        raise RuntimeError("list_leads returned no leads")
    summary = _unwrap(
        await by_name["summarize"].ainvoke({"account_id": case["accounts"][0]})
    )
    if summary.get("error") or not summary.get("transfers"):
        raise RuntimeError("summarize did not return the account's payments")
    walk = _unwrap(
        await by_name["walk_transaction"].ainvoke({"txn_id": case["txn_ids"][0], "depth": 2})
    )
    if walk.get("error") or not walk.get("root"):
        raise RuntimeError("walk_transaction did not start from the payment")
    decision = _unwrap(await by_name["decision_for_lead"].ainvoke({"lead": lead}))
    report = {
        "lead": lead,
        "tools": sorted(by_name),
        "windows": len(windows.get("windows") or []),
        "payments": len(case["transfers"]),
        "graphs": len(explained["graphs"]),
        "leads": len(leads["leads"]),
        "summary_payments": len(summary["transfers"]),
        "walk_steps": len(walk.get("steps") or []),
        "prior_decision": decision.get("decision"),
    }
    _write("mcp-check", report)
    _write("mcp-check-case", case)
    _write("mcp-check-explain", explained)
    return report


def open_agent(tools: list, model=None, log: ConversationLog | None = None, session_id: str = ""):
    """One agent for the whole session. Later turns see the earlier payments."""
    reviewer = [tool for tool in tools if tool.name != "investigate_lead"]
    if log is not None and session_id:
        reviewer.append(workbook_tool(log, GraphReader(log.graph_db), session_id))
    return create_agent(
        model if model is not None else chat_model(),
        reviewer,
        system_prompt=SYSTEM,
        checkpointer=InMemorySaver(),
    )


async def investigate(
    tools: list,
    lead: str,
    question: str,
    *,
    opening: bool = False,
    log: ConversationLog | None = None,
    session_id: str = "",
    model=None,
    agent=None,
    stream: bool = False,
    emit=None,
) -> dict:
    """One turn on the session agent. Pass the same agent for every follow-up.

    `opening` is unused. The question is sent as the officer wrote it.
    `model` replaces the API chat model when no agent is passed in.
    `stream` prints the reply as it is written, or passes each piece to `emit`.
    """
    del opening
    if agent is None:
        agent = open_agent(tools, model)
    message = question or f"Open the payments for {lead}."
    messages = await _invoke(
        agent,
        message,
        session_id or "operator",
        stream=stream,
        emit=emit,
    )
    history = [_message(item) for item in messages]
    turn = _latest_turn(history)
    by_name = {tool.name: tool for tool in tools}
    recorded = _recorded_lead(turn) or lead
    stored = (
        _unwrap(await by_name["decision_for_lead"].ainvoke({"lead": recorded}))
        if recorded
        else {}
    )
    answer = _plain(_answer(turn))
    _write("transcript", {"lead": recorded, "messages": history})
    _write("decision", stored)
    if log is not None and session_id:
        txn_ids, accounts = cited_from_messages(turn)
        calls = calls_from_messages(turn)
        log.append(session_id, "operator", message)
        log.append(
            session_id,
            "agent",
            answer,
            calls=calls,
            txn_ids=txn_ids,
            accounts=accounts,
        )
        refresh_workbook(log, GraphReader(log.graph_db), session_id, calls)
    return {
        "lead": recorded,
        "decision": stored.get("decision"),
        "reason": stored.get("reason"),
        "answer": answer,
        "messages": turn,
    }


async def _invoke(agent, message: str, thread_id: str, *, stream: bool, emit=None):
    config = {
        "recursion_limit": 40,
        "configurable": {"thread_id": thread_id},
    }
    payload = {"messages": [{"role": "user", "content": message}]}
    if not stream:
        result = await agent.ainvoke(payload, config=config)
        return result["messages"]
    final = None
    started = False
    async for mode, chunk in agent.astream(payload, config=config, stream_mode=["messages", "values"]):
        if mode == "values":
            final = chunk
            continue
        if mode != "messages":
            continue
        token = chunk[0] if isinstance(chunk, tuple) else chunk
        if getattr(token, "type", "") not in {"ai", "AIMessageChunk"}:
            continue
        text = _chunk_text(token)
        if not text or getattr(token, "tool_call_chunks", None):
            continue
        if emit is not None:
            sent = emit(text)
            if hasattr(sent, "__await__"):
                await sent
        else:
            print(text, end="", flush=True)
        started = True
    if started and emit is None:
        print(flush=True)
    if isinstance(final, dict) and final.get("messages"):
        return final["messages"]
    state = await agent.aget_state(config)
    return state.values["messages"]


def _chunk_text(token) -> str:
    content = getattr(token, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") in {None, "text"}:
                parts.append(str(block.get("text") or ""))
        return "".join(parts)
    return ""


def _latest_turn(messages: list[dict]) -> list[dict]:
    start = 0
    for index, message in enumerate(messages):
        if message["type"] in {"human", "user"}:
            start = index
    return messages[start:]


def _recorded_lead(messages: list[dict]) -> str:
    for message in reversed(messages):
        for call in message.get("tool_calls") or []:
            if call.get("name") == "record_decision":
                return str((call.get("args") or {}).get("lead") or "")
    return ""


def _plain(text: str) -> str:
    text = re.sub(r"(?m)^#{1,6}\s*", "", text)
    text = text.replace("**", "").replace("__", "").replace("`", "")
    text = re.sub(r"(?m)^\s*[-*]\s+", "", text)
    text = re.sub(r"(?m)^---+\s*$", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _answer(messages: list[dict]) -> str:
    for message in reversed(messages):
        if message["type"] != "ai" or not message.get("content"):
            continue
        content = message["content"]
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "".join(
                block.get("text", "") if isinstance(block, dict) else str(block)
                for block in content
            )
    return ""


def _unwrap(result):
    if isinstance(result, list):
        text = "".join(
            block.get("text", "") if isinstance(block, dict) else str(block) for block in result
        )
        return _unwrap(text) if text else {"content": result}
    if isinstance(result, str):
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"text": result}
    if isinstance(result, dict):
        return result
    content = getattr(result, "content", None)
    if isinstance(content, str):
        return _unwrap(content)
    if isinstance(content, list):
        text = "".join(
            block.get("text", "") if isinstance(block, dict) else str(block) for block in content
        )
        return _unwrap(text) if text else {"content": content}
    return json.loads(json.dumps(result, default=str))


def _message(message) -> dict:
    payload = {"type": message.type, "content": message.content}
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        payload["tool_calls"] = tool_calls
    name = getattr(message, "name", None)
    if name:
        payload["name"] = name
    return payload


def _write(name: str, payload: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.json").write_text(json.dumps(payload, indent=2, default=str) + "\n")
