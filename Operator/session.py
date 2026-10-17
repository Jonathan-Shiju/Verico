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
REPLY = (Path(__file__).resolve().parent / "reply.md").read_text()
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

When the reply shows payments, call flags_for_account for each account in those payments and write the section Suspicious activity. When the reply is one account or one party, call flags_for_account for that account and write Account caution. A mark is many accounts sending money to this one, this one sending money to many accounts, or one payment that left matching the payments that arrived. Say it that way. Do not say the mark's stored name. A mark is not a lead and not a finding of fraud. If the call returns no mark, say there is no mark. Do not invent one.

Use the tools to read, then answer in that voice. The tool names below are for you only and must not appear in the reply. Leads are opened with case_file. An account summary uses summarize. Later payments from one payment use walk_transaction. How a set of payments connects uses explain_transactions. When they name one lead, open that lead. Do not also list every other lead unless they ask. When the officer asks what you think, give that reading from the payments already in the conversation. When they say keep the lead, or keep or dismiss, call record_decision with the payment ids and accounts already read. Do not ask them to supply those ids. keep means a person should look at the payments. dismiss means the payments read as ordinary. The reason must mention each payment id you record. When they ask to list leads or for more leads, call list_leads with limit 3 and, for a later page, skip past the ones already shown. Describe at most three leads in the markdown shape, and say that more exist. Do not recite every account the tool returned. When they ask to flag an account, or to open the accounts that dealt with it, call open_counterparty_leads for that account. That writes one lead for each of those accounts, separate from a loop and from payments that add up. The reply uses the Flagged shape. Name the flagged party and every account the tool returned, with the payments between them. Do not call any of them the first lead, the second lead, or another account. Open one of those leads with case_file when they want the payments between the two accounts, and describe those payments in the markdown shape. A case book is the workbook. When they ask to open the case book, or for a workbook, call open_workbook with the lead already open. If the lead has not been kept, call record_decision for that lead first, then open_workbook. Do not call investigate_lead.

After the tools, the final message is only this JSON object. Do not write the markdown before it. No code fence. The reply field is the markdown. It does not ask the officer to choose or to name an account. The two question headings in the shape are allowed.
{"reply":"<markdown>","prompts":"<three commands the officer would type next, separated by a comma and a space>"}
prompts are commands in the officer's voice, not questions back to them. Do not start one with would you, could you, or can you. Do not ask them to clarify or to name an account. Separate the commands with a comma and a space. Do not put a comma inside a command. Never put a payment id or an account id in a command. Name a lead as the first lead, the second lead, or a different lead.
When the reply just flagged an account, each command opens the payments with one named party from that reply. Do not call those the first lead.
When no lead is open, each command opens one lead from this reply, or looks at a different lead.
When a lead is open and has not been kept, the commands begin Keep the lead, Open the case book, and the third looks at the accounts in the reply or flags one of those accounts and the accounts that dealt with it.
When the lead is kept and the case book is not open, the first command is Open the case book, then look at those accounts or flag one account and the accounts that dealt with it.
Do not repeat the command the officer just sent.
""" + REPLY


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
    if leads.get("error"):
        raise RuntimeError("list_leads failed: " + str(leads["error"]))
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
    tap = _ReplyTap()

    async def forward(piece: str) -> None:
        visible = tap.push(piece)
        if not visible:
            return
        if emit is not None:
            sent = emit(visible)
            if hasattr(sent, "__await__"):
                await sent
        elif stream:
            print(visible, end="", flush=True)

    messages = await _invoke(
        agent,
        message,
        session_id or "operator",
        stream=stream,
        emit=forward if stream else None,
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
    answer, prompts = _spoken(_answer(turn))
    answer = _reply_text(answer)
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
        "prompts": prompts,
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


class _ReplyTap:
    """Show only the reply field while a JSON answer is still being written."""

    def __init__(self) -> None:
        self.buf = ""
        self.shown = ""

    def push(self, piece: str) -> str:
        self.buf += piece
        reply = _partial_reply(self.buf)
        if reply is None:
            return ""
        if reply.startswith(self.shown):
            fresh = reply[len(self.shown) :]
            self.shown = reply
            return fresh
        self.shown = reply
        return reply


def _partial_reply(buf: str) -> str | None:
    match = re.search(r'"reply"\s*:\s*"', buf)
    if not match:
        return None
    index = match.end()
    chars: list[str] = []
    while index < len(buf):
        char = buf[index]
        if char == "\\":
            if index + 1 >= len(buf):
                break
            nxt = buf[index + 1]
            if nxt == "u":
                if index + 6 > len(buf):
                    break
                try:
                    chars.append(chr(int(buf[index + 2 : index + 6], 16)))
                except ValueError:
                    break
                index += 6
                continue
            chars.append({"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\"}.get(nxt, nxt))
            index += 2
            continue
        if char == '"':
            break
        chars.append(char)
        index += 1
    return "".join(chars)


def _spoken(raw: str) -> tuple[str, list[str]]:
    """Split the final message into the spoken reply and the officer's next commands."""
    text = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1).strip()
    start, end = text.find("{"), text.rfind("}")
    payload = None
    if start >= 0 and end > start:
        try:
            payload = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            payload = None
    if isinstance(payload, dict) and "reply" in payload:
        return str(payload.get("reply") or "").strip(), _prompt_list(payload.get("prompts"))
    reply = _json_string(text, "reply")
    prompts = _json_string(text, "prompts")
    if reply is not None:
        return reply, _prompt_list(prompts or "")
    return text, []


def _json_string(text: str, key: str) -> str | None:
    match = re.search(rf'"{key}"\s*:\s*"((?:\\.|[^"\\])*)"', text, re.S)
    if not match:
        return None
    try:
        return json.loads(f'"{match.group(1)}"')
    except json.JSONDecodeError:
        return match.group(1)


def _prompt_list(value) -> list[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        items = _split_commands(value)
    else:
        return []
    prompts = []
    for item in items:
        prompt = " ".join(str(item).split()).strip(" .")
        if prompt and prompt not in prompts:
            prompts.append(prompt[:180])
    return prompts


_COMMANDS = {
    "open",
    "look",
    "keep",
    "flag",
    "show",
    "let's",
    "lets",
    "dismiss",
    "list",
    "read",
    "review",
}


def _split_commands(value: str) -> list[str]:
    """Split on commas, and keep a comma that sits inside one command."""
    commands: list[str] = []
    for part in value.split(","):
        part = " ".join(part.split())
        if not part:
            continue
        if _starts_command(part):
            commands.append(part)
        elif commands:
            commands[-1] = f"{commands[-1]}, {part}"
    return commands


def _starts_command(part: str) -> bool:
    word = part.split(" ", 1)[0].casefold().strip(".'\"")
    return word in _COMMANDS


def _reply_text(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:markdown|md)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
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
