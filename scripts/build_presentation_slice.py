"""Cut a small payment file for the t3.small presentation graph.

The file keeps a few leads the full graph already stored, one labeled
laundering pattern that is not a lead, and ordinary payments on other
accounts. It does not write to Falkor or Redis.
"""

import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from GraphIngest.Accounts import account_id  # noqa: E402
from scripts.evaluate_loops import load_patterns, pattern_key  # noqa: E402

TRANS = PROJECT_ROOT / "transactions_files" / "HI-Small_Trans.csv"
ACCOUNTS = PROJECT_ROOT / "transactions_files" / "HI-Small_accounts.csv"
PATTERNS = PROJECT_ROOT / "transactions_files" / "HI-Small_Patterns.txt"
OUT = PROJECT_ROOT / "presentation" / "slice"
ORDINARY = 8000

# Leads already opened in the full graph. Row number in HI-Small is the txn id.
KEPT_LOOP = (325411, 3932103)
EARLY_LOOP = (393953, 477907)
POOLED = (127427, 1615971, 1693885)
SPLIT = (1117840, 1828122, 2121045)


def main() -> None:
    loop_ids = _ids("aml:loops", "MATCH (l:Loop) RETURN l.txn_ids")
    case_ids = _case_ids()
    patterns = load_patterns(PATTERNS)
    wanted = _wanted_keys(patterns)
    rows_by_id, pattern_hits = _scan(wanted)
    _check_known(rows_by_id)

    fanout = _pick_pattern(patterns, pattern_hits, "FAN-OUT", loop_ids, case_ids)
    cycle = _pick_pattern(patterns, pattern_hits, "CYCLE", loop_ids, case_ids)
    stories = _stories(rows_by_id, fanout, cycle, loop_ids, case_ids)
    showcase_ids = {txn_id for story in stories for txn_id in story["txn_ids"]}
    showcase_accounts = {account for story in stories for account in story["accounts"]}
    ordinary = _ordinary(showcase_accounts)
    selected = {txn_id: rows_by_id[txn_id] for txn_id in showcase_ids}
    selected.update(ordinary)
    for txn_id in showcase_ids:
        if txn_id not in selected:
            raise RuntimeError(f"Missing payment {txn_id}")

    OUT.mkdir(parents=True, exist_ok=True)
    _write_payments(selected)
    account_count = _write_accounts(selected)
    manifest = {
        "payments": len(selected),
        "accounts": account_count,
        "ordinary_payments": len(ordinary),
        "window": "8-day windows, a new one every 4 days, as on the full graph. The loops and residues sit in the window that starts 2022/09/01. The labeled fan-out and cycle sit in the window that starts 2022/09/05.",
        "stories": stories,
    }
    (OUT / "slice.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"payments={len(selected)} accounts={account_count} ordinary={len(ordinary)}")
    for story in stories:
        print(
            story["role"],
            story["kind"],
            "txns",
            len(story["txn_ids"]),
            "labeled",
            story["labeled_payments"],
            "in_a_stored_lead",
            story["in_a_stored_lead"],
        )


def _ids(graph: str, query: str) -> set[int]:
    found = set()
    for row in FalkorGraphDB().read(graph, query).result_set:
        raw = row[0]
        if isinstance(raw, str):
            found.update(int(part) for part in raw.split(",") if part)
        elif raw:
            found.update(int(part) for part in raw)
    return found


def _case_ids() -> set[int]:
    found = set()
    rows = FalkorGraphDB().read(
        "aml:cases", "MATCH (c:Case) RETURN c.focal_txn, c.part_txns"
    ).result_set
    for focal, parts in rows:
        found.add(int(focal))
        if parts:
            found.update(int(part) for part in str(parts).split(",") if part)
    return found


def _wanted_keys(patterns: list) -> dict[tuple, list[tuple[str, int, int]]]:
    wanted = {}
    for index, (kind, keys) in enumerate(patterns):
        if kind not in {"FAN-OUT", "CYCLE"}:
            continue
        for hop, key in enumerate(keys):
            wanted.setdefault(key, []).append((kind, index, hop))
    return wanted


def _scan(wanted: dict) -> tuple[dict[int, dict], dict[tuple[str, int], dict[int, int]]]:
    needed = set(KEPT_LOOP + EARLY_LOOP + POOLED + SPLIT)
    rows_by_id = {}
    hits: dict[tuple[str, int], dict[int, int]] = {}
    with open(TRANS, newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        for txn_id, fields in enumerate(reader):
            payment = _payment(txn_id, fields)
            if txn_id in needed:
                rows_by_id[txn_id] = payment
            key = pattern_key(fields[0], fields[1], fields[2], fields[3], fields[4], fields[7])
            for kind, index, hop in wanted.get(key, []):
                hits.setdefault((kind, index), {})[hop] = txn_id
                rows_by_id[txn_id] = payment
    return rows_by_id, hits


def _payment(txn_id: int, fields: list[str]) -> dict:
    return {
        "txn_id": txn_id,
        "timestamp": fields[0],
        "from_bank": fields[1],
        "from_account": fields[2],
        "to_bank": fields[3],
        "to_account": fields[4],
        "amount_received": fields[5],
        "receiving_currency": fields[6],
        "amount_paid": fields[7],
        "payment_currency": fields[8],
        "payment_format": fields[9],
        "is_laundering": int(fields[10]),
        "src": account_id(fields[1], fields[2]),
        "dst": account_id(fields[3], fields[4]),
    }


def _check_known(rows_by_id: dict[int, dict]) -> None:
    payment = rows_by_id[325411]
    if payment["src"] != "54219:81464BEA1" or payment["dst"] != "254958:8146532E1":
        raise RuntimeError("txn 325411 is not the kept loop's first payment")


def _pick_pattern(patterns, hits, kind: str, loop_ids: set[int], case_ids: set[int]):
    """The longest labeled pattern of this kind that is not already a lead.

    A fan-out shorter than 8 payments is below the degree flag, so it would
    not read as a fan-out on the small graph. A cycle shorter than 6 hops is
    kept only when no longer one is clear of the stored leads.
    """
    floor = 8 if kind == "FAN-OUT" else 6
    best = None
    for index, (pattern_kind, keys) in enumerate(patterns):
        if pattern_kind != kind:
            continue
        found = hits.get((kind, index)) or {}
        if len(found) != len(keys) or set(found) != set(range(len(keys))):
            continue
        txn_ids = [found[hop] for hop in range(len(keys))]
        if any(txn_id in loop_ids or txn_id in case_ids for txn_id in txn_ids):
            continue
        if len(txn_ids) < floor:
            continue
        if best is None or len(txn_ids) > len(best[0]):
            best = (txn_ids, index)
    if best is None:
        raise RuntimeError(f"No {kind} pattern of at least {floor} payments sits outside the stored leads")
    return best


def _stories(rows_by_id, fanout, cycle, loop_ids, case_ids) -> list[dict]:
    stories = [
        _lead_story("detected_loop", "loop", KEPT_LOOP, rows_by_id, loop_ids, case_ids),
        _lead_story("detected_loop", "loop", EARLY_LOOP, rows_by_id, loop_ids, case_ids),
        _lead_story("detected_residue", "pooled", POOLED, rows_by_id, loop_ids, case_ids),
        _lead_story("detected_residue", "split", SPLIT, rows_by_id, loop_ids, case_ids),
        _pattern_story("undetected_laundering", "FAN-OUT", fanout[0], rows_by_id),
        _pattern_story("undetected_laundering", "CYCLE", cycle[0], rows_by_id),
    ]
    return stories


def _lead_story(role, kind, txn_ids, rows_by_id, loop_ids, case_ids) -> dict:
    payments = [rows_by_id[txn_id] for txn_id in txn_ids]
    return _story(role, kind, payments, loop_ids, case_ids)


def _pattern_story(role, kind, txn_ids, rows_by_id) -> dict:
    payments = [rows_by_id[txn_id] for txn_id in txn_ids]
    return _story(role, kind, payments, set(), set())


def _story(role, kind, payments, loop_ids, case_ids) -> dict:
    accounts = []
    for payment in payments:
        for account in (payment["src"], payment["dst"]):
            if account not in accounts:
                accounts.append(account)
    txn_ids = [payment["txn_id"] for payment in payments]
    return {
        "role": role,
        "kind": kind,
        "txn_ids": txn_ids,
        "accounts": accounts,
        "start": payments[0]["timestamp"],
        "end": payments[-1]["timestamp"],
        "labeled_payments": sum(payment["is_laundering"] for payment in payments),
        "in_a_stored_lead": any(txn_id in loop_ids or txn_id in case_ids for txn_id in txn_ids),
    }


def _ordinary(blocked: set[str]) -> dict[int, dict]:
    found = {}
    with open(TRANS, newline="") as handle:
        reader = csv.reader(handle)
        next(reader)
        seen = 0
        for txn_id, fields in enumerate(reader):
            day = fields[0][:10]
            if day < "2022/09/01":
                continue
            if day > "2022/09/04":
                break
            if fields[10] != "0":
                continue
            src = account_id(fields[1], fields[2])
            dst = account_id(fields[3], fields[4])
            if src == dst or src in blocked or dst in blocked:
                continue
            seen += 1
            if seen % 5 != 0:
                continue
            found[txn_id] = {
                "txn_id": txn_id,
                "timestamp": fields[0],
                "from_bank": fields[1],
                "from_account": fields[2],
                "to_bank": fields[3],
                "to_account": fields[4],
                "amount_received": fields[5],
                "receiving_currency": fields[6],
                "amount_paid": fields[7],
                "payment_currency": fields[8],
                "payment_format": fields[9],
                "is_laundering": 0,
                "src": src,
                "dst": dst,
            }
            if len(found) >= ORDINARY:
                break
    if len(found) < ORDINARY:
        raise RuntimeError(f"Only found {len(found)} ordinary payments")
    return found


def _write_payments(selected: dict[int, dict]) -> None:
    header = [
        "Timestamp",
        "From Bank",
        "Account",
        "To Bank",
        "Account",
        "Amount Received",
        "Receiving Currency",
        "Amount Paid",
        "Payment Currency",
        "Payment Format",
        "Is Laundering",
        "Txn Id",
    ]
    with open(OUT / "payments.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for txn_id in sorted(selected):
            payment = selected[txn_id]
            writer.writerow(
                [
                    payment["timestamp"],
                    payment["from_bank"],
                    payment["from_account"],
                    payment["to_bank"],
                    payment["to_account"],
                    payment["amount_received"],
                    payment["receiving_currency"],
                    payment["amount_paid"],
                    payment["payment_currency"],
                    payment["payment_format"],
                    payment["is_laundering"],
                    txn_id,
                ]
            )


def _write_accounts(selected: dict[int, dict]) -> int:
    needed = set()
    for payment in selected.values():
        needed.add(payment["src"])
        needed.add(payment["dst"])
    kept = 0
    with open(ACCOUNTS, newline="") as source, open(OUT / "accounts.csv", "w", newline="") as target:
        reader = csv.DictReader(source)
        writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if account_id(row["Bank ID"], row["Account Number"]) in needed:
                writer.writerow(row)
                kept += 1
    if kept != len(needed):
        raise RuntimeError(f"Registry has {kept} of {len(needed)} slice accounts")
    return kept


if __name__ == "__main__":
    main()
