"""Check the MCP server, then answer the operator.

The operator starts by asking for leads, a summary of an account or an entity,
or a walk down from a transaction id. The agent uses the other tools after that.
The model decides which tool to call, including the workbook.

Usage:
    .venv/bin/python scripts/operator_pipeline.py
    .venv/bin/python scripts/operator_pipeline.py --question "List the leads I can open."
    .venv/bin/python scripts/operator_pipeline.py 393953,477907

The session needs HF_TOKEN, GEMINI_API_KEY, or OPENAI_API_KEY for an answer.
A Hugging Face model calls the local tools through the agent. The MCP check
runs first. Replies are written to presentation/operator-pipeline.
"""

import argparse
import asyncio
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
from Operator.session import check_mcp, investigate, open_agent, server_client  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser(description="Operator investigation through MCP")
    parser.add_argument("lead", nargs="?", default="393953,477907")
    parser.add_argument("--question", default="")
    parser.add_argument("--session", default="")
    args = parser.parse_args()

    question = args.question
    if args.lead and not question:
        question = f"Open lead {args.lead}."
    if not question:
        question = "List the leads I can open."
    session_id = args.session or uuid4().hex
    log = ConversationLog(Redis(), FalkorGraphDB())

    async with MCPAdapter(server_client()) as adapter:
        tools = await adapter.list_tools()
        report = await check_mcp(tools, args.lead)
        print(
            f"mcp ok tools={len(report['tools'])} windows={report['windows']} "
            f"leads={report['leads']} payments={report['payments']} "
            f"summary={report['summary_payments']} walk_steps={report['walk_steps']}"
        )
        agent = open_agent(tools, log=log, session_id=session_id)
        print("session", session_id)
        answer = await investigate(
            tools,
            args.lead,
            question,
            log=log,
            session_id=session_id,
            agent=agent,
            stream=True,
        )
        if answer.get("decision"):
            print(answer["decision"], answer.get("reason") or "")
        if sys.stdin.isatty():
            while True:
                try:
                    follow = input("operator> ").strip()
                except EOFError:
                    break
                if follow.lower() in {"", "close", "quit", "exit"}:
                    break
                answer = await investigate(
                    tools,
                    args.lead,
                    follow,
                    log=log,
                    session_id=session_id,
                    agent=agent,
                    stream=True,
                )
                if answer.get("decision"):
                    print(answer["decision"], answer.get("reason") or "")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as err:
        raise SystemExit(str(err)) from None
