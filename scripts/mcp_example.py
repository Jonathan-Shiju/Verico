"""Call the aml-graph MCP tools and write each reply to presentation/mcp-example.

The script starts GraphRead/server.py over stdio, the same way Cursor does, and
talks to it only through tools/call. It opens one stored loop, then uses the
account and transaction ids that call returns.
"""

import asyncio
import json
from pathlib import Path

from mcp import Client
from mcp.client.stdio import StdioServerParameters

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "presentation" / "mcp-example"
LEAD = "393953,477907"


def _body(result) -> dict:
    if result.structured_content is not None:
        return result.structured_content
    text = "".join(getattr(block, "text", "") for block in result.content)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def _write(name: str, payload: dict) -> None:
    path = OUT / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    print(name, path.name)


async def _call(client: Client, name: str, arguments: dict) -> dict:
    result = await client.call_tool(name, arguments)
    payload = _body(result)
    _write(name, payload)
    return payload


async def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    server = StdioServerParameters(
        command=str(ROOT / ".venv" / "bin" / "python"),
        args=[str(ROOT / "GraphRead" / "server.py")],
        cwd=ROOT,
    )
    async with Client(server, read_timeout_seconds=120) as client:
        windows = await _call(client, "list_windows", {})
        case = await _call(client, "case_file", {"lead": LEAD})
        account = case["accounts"][0]
        window = case["windows"][0]
        txn_ids = case["txn_ids"]

        await _call(client, "loops_for_account", {"account_id": account, "limit": 5})
        await _call(client, "cases_for_account", {"account_id": account, "limit": 5})
        await _call(client, "account_activity", {
            "account_id": account,
            "window": window,
            "limit": 5,
        })
        await _call(client, "one_hop_across_windows", {
            "account_id": account,
            "window": window,
            "counterparty_limit": 3,
            "limit": 5,
        })
        await _call(client, "flags_for_account", {"account_id": account, "limit": 5})
        await _call(client, "query_cypher", {
            "graph": window,
            "limit": 5,
            "cypher": (
                "MATCH (a:Account {id: '" + account + "'})-[t:TRANSFER]->(b:Account) "
                "RETURN b.id AS dst, t.txn_id AS txn_id, t.amount_paid AS amount_paid "
                "LIMIT 5"
            ),
        })
        await _call(client, "explain_transactions", {"txn_ids": txn_ids})
        await _call(client, "decision_for_lead", {"lead": LEAD})
        await _call(client, "investigate_lead", {"lead": LEAD})

    print("windows", len(windows.get("windows", [])))
    print("wrote", OUT)


if __name__ == "__main__":
    asyncio.run(main())
