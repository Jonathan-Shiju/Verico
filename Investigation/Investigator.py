"""One model call for one lead. The call is not on the transaction path."""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

import redis
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from GraphRead.reader import GraphReader, _rows
from Investigation.Decisions import DecisionStore

HOP_COUNTERPARTIES = 5
HOP_TRANSFERS = 8

# A parameter in the self-transfer check makes this scan the window. The
# comparison is written out so the read stays on the one account.
ACCOUNT_EDGES = """
MATCH (a:Account {id: $id})-[t:TRANSFER]->(b:Account)
WHERE a.id <> b.id
RETURN a.id AS src, b.id AS dst, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.payment_format AS payment_format
ORDER BY t.ts DESC
LIMIT $limit
UNION
MATCH (b:Account)-[t:TRANSFER]->(a:Account {id: $id})
WHERE a.id <> b.id
RETURN b.id AS src, a.id AS dst, t.txn_id AS txn_id, t.ts AS ts,
       t.amount_paid AS amount_paid, t.payment_currency AS payment_currency,
       t.payment_format AS payment_format
ORDER BY t.ts DESC
LIMIT $limit
"""


def dossier(reader: GraphReader, lead: str) -> dict:
    """The case file, plus one hop when the lead does not close."""
    case = reader.case_file(lead)
    if case.get("error"):
        return case
    followed = None
    if case.get("open") and case.get("windows"):
        account = case.get("account") or case["accounts"][-1]
        followed = _follow(reader, account, case["windows"][-1])
    return {
        "lead": case["lead"],
        "kind": case["kind"],
        "open": bool(case.get("open")),
        "accounts": case.get("accounts") or [],
        "windows": case.get("windows") or [],
        "transfers": [_payment(row) for row in case.get("transfers") or []],
        "followed": followed,
    }


def review(reader: GraphReader, store: DecisionStore, lead: str, complete=None) -> dict:
    """Read one lead, ask the model, and store keep or dismiss.

    `complete` turns the prompt into the model's text. The default calls the
    configured API. A decision is stored only after its cited transactions and
    accounts are ones this call actually read.
    """
    packet = dossier(reader, lead)
    if packet.get("error"):
        return packet
    allowed_txns, allowed_accounts = _evidence(packet)
    if not allowed_txns:
        return {"lead": lead, "error": "The lead has no payments to read"}
    try:
        text = (complete or model_complete)(_prompt(packet))
        parsed = parse_decision(text, allowed_txns, allowed_accounts)
    except (ValueError, json.JSONDecodeError, urllib.error.URLError, TimeoutError) as err:
        return {"lead": lead, "error": str(err)}
    stored = store.write(
        lead,
        parsed["decision"],
        packet["kind"],
        parsed["reason"],
        parsed["accounts"],
        parsed["txn_ids"],
    )
    stored["graph"] = "aml:decisions"
    return stored


def parse_decision(text: str, allowed_txns: set[int], allowed_accounts: set[str]) -> dict:
    payload = json.loads(_json_text(text))
    decision = payload.get("decision")
    if decision not in ("keep", "dismiss"):
        raise ValueError("decision must be keep or dismiss")
    try:
        txn_ids = [int(txn_id) for txn_id in payload.get("txn_ids") or []]
    except (TypeError, ValueError) as err:
        raise ValueError("txn_ids must be transaction ids from the dossier") from err
    if not txn_ids or any(txn_id not in allowed_txns for txn_id in txn_ids):
        raise ValueError("txn_ids must be transaction ids this call read")
    accounts = [str(account) for account in payload.get("accounts") or []]
    if decision == "dismiss" and accounts:
        raise ValueError("dismiss does not raise accounts")
    if decision == "keep" and not accounts:
        raise ValueError("keep names at least one account")
    if any(account not in allowed_accounts for account in accounts):
        raise ValueError("accounts must be accounts this call read")
    reason = str(payload.get("reason") or "").strip()
    if not reason or any(str(txn_id) not in reason for txn_id in txn_ids):
        raise ValueError("reason must cite each transaction id it relies on")
    return {
        "decision": decision,
        "accounts": accounts,
        "txn_ids": txn_ids,
        "reason": reason,
    }


def record(
    reader: GraphReader,
    store: DecisionStore,
    lead: str,
    decision: str,
    accounts: list[str],
    txn_ids: list[int],
    reason: str,
) -> dict:
    """Store an operator decision after checking the cited payments exist.

    The cited payments are loaded the same way explain_transactions loads them.
    A keep can name only accounts on those payments.
    """
    from GraphRead.explain import explain_transactions

    try:
        cited = _txn_ids(txn_ids)
        named = _accounts(accounts)
    except (TypeError, ValueError):
        return {"lead": lead, "error": "accounts and txn_ids must be ids"}
    explained = explain_transactions(reader, cited)
    if explained.get("error"):
        return {"lead": lead, "error": explained["error"]}
    if explained.get("missing"):
        return {"lead": lead, "error": "txn_ids must be payments that exist"}
    allowed_txns: set[int] = set()
    allowed_accounts: set[str] = set()
    for graph in explained["graphs"]:
        for account in graph["accounts"]:
            allowed_accounts.add(account["id"])
        for transfer in graph["transfers"]:
            allowed_txns.add(int(transfer["txn_id"]))
            allowed_accounts.add(transfer["src"])
            allowed_accounts.add(transfer["dst"])
    try:
        parsed = parse_decision(
            json.dumps(
                {
                    "decision": decision,
                    "accounts": named,
                    "txn_ids": cited,
                    "reason": reason,
                }
            ),
            allowed_txns,
            allowed_accounts,
        )
    except (ValueError, json.JSONDecodeError) as err:
        return {"lead": lead, "error": str(err)}
    stored = store.write(
        lead,
        parsed["decision"],
        "operator",
        parsed["reason"],
        parsed["accounts"],
        parsed["txn_ids"],
    )
    stored["graph"] = "aml:decisions"
    stored["graphs"] = len(explained["graphs"])
    return stored


def _txn_ids(raw) -> list[int]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        values = list(raw or [])
    return [int(value) for value in values]


def _accounts(raw) -> list[str]:
    if isinstance(raw, str):
        return [part.strip() for part in raw.split(",") if part.strip()]
    return [str(account) for account in (raw or [])]


def model_complete(prompt: str) -> str:
    provider, key, model, url = _model_settings()
    if provider == "huggingface":
        body = {
            "model": model,
            "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        }
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
        )
        raw = _post(request)
        return raw["choices"][0]["message"]["content"]
    if provider == "gemini":
        body = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
        }
        request = urllib.request.Request(
            f"{url}?key={key}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        raw = _post(request)
        return raw["candidates"][0]["content"]["parts"][0]["text"]
    body = {
        "model": model,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {key}"},
    )
    raw = _post(request)
    return raw["choices"][0]["message"]["content"]


def _model_settings() -> tuple[str, str, str, str]:
    provider = os.environ.get("INVESTIGATOR_PROVIDER", "").strip().lower()
    if provider == "hf":
        provider = "huggingface"
    gemini_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    openai_key = os.environ.get("OPENAI_API_KEY")
    hf_key = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if not provider:
        provider = (
            "gemini"
            if gemini_key
            else "openai"
            if openai_key
            else "huggingface"
            if hf_key
            else ""
        )
    if provider == "gemini":
        if not gemini_key:
            raise ValueError("Set GEMINI_API_KEY or GOOGLE_API_KEY to review a lead")
        model = os.environ.get("INVESTIGATOR_MODEL", "gemini-2.5-flash")
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{model}:generateContent"
        )
        return provider, gemini_key, model, url
    if provider == "openai":
        if not openai_key:
            raise ValueError("Set OPENAI_API_KEY to review a lead")
        model = os.environ.get("INVESTIGATOR_MODEL", "gpt-4o-mini")
        return provider, openai_key, model, "https://api.openai.com/v1/chat/completions"
    if provider == "huggingface":
        if not hf_key:
            raise ValueError("Set HF_TOKEN to review a lead")
        model = os.environ.get("INVESTIGATOR_MODEL", "deepseek-ai/DeepSeek-V3:deepinfra")
        return provider, hf_key, model, "https://router.huggingface.co/v1/chat/completions"
    raise ValueError("Set HF_TOKEN, GEMINI_API_KEY, or OPENAI_API_KEY to review a lead")


def _post(request: urllib.request.Request) -> dict:
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as err:
        raise ValueError(f"The model call failed ({err.code})") from None


def _prompt(packet: dict) -> str:
    return (
        "Review this one lead. Return only a JSON object with the keys "
        "decision, accounts, txn_ids, and reason.\n"
        "decision is keep or dismiss. keep means a person should look at this "
        "lead. dismiss means the payments read as ordinary.\n"
        "accounts lists account ids from the dossier to raise, and is empty "
        "when the decision is dismiss.\n"
        "txn_ids lists the transaction ids that support the decision. Every id "
        "must appear in the dossier.\n"
        "reason is a short explanation and includes each of those transaction ids.\n"
        "DOSSIER:\n"
        + json.dumps(packet, default=str)
    )


def _evidence(packet: dict) -> tuple[set[int], set[str]]:
    txn_ids = {row["txn_id"] for row in packet["transfers"]}
    accounts = set(packet["accounts"])
    followed = packet.get("followed") or {}
    if followed.get("account"):
        accounts.add(followed["account"])
    for row in followed.get("transfers") or []:
        txn_ids.add(row["txn_id"])
        accounts.add(row["account"])
        if row.get("other"):
            accounts.add(row["other"])
    for row in packet["transfers"]:
        accounts.add(row["src"])
        accounts.add(row["dst"])
    return txn_ids, accounts


def _payment(row: dict) -> dict:
    return {
        "txn_id": int(row["txn_id"]),
        "src": row.get("src"),
        "dst": row.get("dst"),
        "amount_paid": row.get("amount_paid"),
        "payment_currency": row.get("payment_currency"),
        "payment_format": row.get("payment_format"),
        "time": row.get("time"),
        "window": row.get("window"),
    }


def _follow(reader: GraphReader, account: str, window: str) -> dict:
    """Payments on the account, then on a few counterparties in the neighboring windows.

    Each read starts at an account. A window-wide lookup by transaction id times out.
    """
    adjacent = _adjacent(reader, window)
    transfers = _account_edges(reader, account, window)
    others = []
    for row in transfers:
        other = row.get("other")
        if other and other not in others:
            others.append(other)
        if len(others) >= HOP_COUNTERPARTIES:
            break
    for other in others:
        for graph in adjacent:
            transfers.extend(_account_edges(reader, other, graph))
    return {
        "account": account,
        "window": window,
        "adjacent_windows": adjacent,
        "transfers": transfers,
    }


def _account_edges(reader: GraphReader, account: str, window: str) -> list[dict]:
    try:
        rows = _rows(
            reader.graph_db.read(
                window, ACCOUNT_EDGES, {"id": account, "limit": HOP_TRANSFERS}
            ),
            HOP_TRANSFERS,
        )["rows"]
    except redis.ResponseError:
        return []
    return [_hop_payment(account, window, row) for row in rows]


def _adjacent(reader: GraphReader, window: str) -> list[str]:
    names = [item["graph"] for item in reader.list_windows()["windows"]]
    if window not in names:
        return []
    index = names.index(window)
    return [names[i] for i in (index - 1, index + 1) if 0 <= i < len(names)]


def _hop_payment(account: str, graph: str, row: dict) -> dict:
    src = row.get("src")
    dst = row.get("dst")
    if src is not None and dst is not None:
        other = dst if src == account else src
        direction = "out" if src == account else "in"
    else:
        other = row.get("other")
        direction = row.get("direction")
    return {
        "txn_id": int(row["txn_id"]),
        "account": account,
        "other": other,
        "direction": direction,
        "amount_paid": row.get("amount_paid"),
        "payment_currency": row.get("payment_currency"),
        "payment_format": row.get("payment_format"),
        "time": row.get("time"),
        "window": graph,
    }


def _json_text(text: str) -> str:
    body = text.strip()
    if body.startswith("```"):
        body = body.split("\n", 1)[1]
        body = body.rsplit("```", 1)[0]
    return body.strip()
