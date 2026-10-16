"""Runs the tree loop detector offline over a stored dataset and scores the cycles
against the dataset's labels and laundering-pattern file. Labels are only used
here, never by the detector."""

import argparse
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.staticdb import Redis  # noqa: E402
from Detection.TreeLoopDetector import TreeConfig, TreeLoopDetector  # noqa: E402
from GraphIngest.GraphIngestor import to_row  # noqa: E402
from StreamReplay.TransactionReplay import TransactionStore  # noqa: E402


def pattern_key(timestamp, from_bank, from_acct, to_bank, to_acct, amount_paid) -> tuple:
    return (timestamp, from_bank, from_acct, to_bank, to_acct, round(float(amount_paid), 2))


def load_patterns(path: Path) -> list[tuple[str, list[tuple]]]:
    patterns, current = [], None
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("BEGIN"):
            current = (line.split(" - ", 1)[1].split(":")[0].strip(), [])
        elif line.startswith("END"):
            if current:
                patterns.append(current)
            current = None
        elif line and current:
            f = line.split(",")
            current[1].append(pattern_key(f[0], f[1], f[2], f[3], f[4], f[7]))
    return patterns


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="HI-Small_Trans")
    parser.add_argument(
        "--patterns",
        type=Path,
        default=PROJECT_ROOT / "transactions_files" / "HI-Small_Patterns.txt",
    )
    parser.add_argument(
        "--until", default=None, help="Only process events before this timestamp"
    )
    for name in (
        "max_depth",
        "spread_limit",
        "max_leaves_per_account",
        "max_children_per_leaf",
        "max_expansions_per_root",
    ):
        parser.add_argument(f"--{name.replace('_', '-')}", dest=name, type=int)
    for name in ("min_flow_fraction", "amount_tolerance"):
        parser.add_argument(f"--{name.replace('_', '-')}", dest=name, type=float)
    args = parser.parse_args()

    overrides = {
        k: v
        for k, v in vars(args).items()
        if k in TreeConfig.__dataclass_fields__ and v is not None
    }
    detector = TreeLoopDetector(TreeConfig(**overrides))
    labels: dict[int, int] = {}
    key_to_txn: dict[tuple, int] = {}
    txn_accounts: dict[int, tuple[str, str]] = {}
    cycles = []
    started = time.monotonic()
    for _, transactions in TransactionStore(Redis()).iter_minutes(args.dataset):
        if args.until and transactions[0]["timestamp"] >= args.until:
            break
        for t in transactions:
            row = to_row(t)
            labels[row["txn_id"]] = t["is_laundering"]
            if t["is_laundering"]:
                key_to_txn[
                    pattern_key(
                        t["timestamp"], t["from_bank"], t["from_account"],
                        t["to_bank"], t["to_account"], t["amount_paid"],
                    )
                ] = row["txn_id"]
                txn_accounts[row["txn_id"]] = (row["src"], row["dst"])
            cycles.extend(detector.process(row))
    elapsed = time.monotonic() - started

    laundering_accounts = {a for pair in txn_accounts.values() for a in pair}
    flagged = {a for c in cycles for a in c.accounts}
    pure = sum(all(labels[t] for t in c.txn_ids) for c in cycles)
    touched = sum(any(labels[t] for t in c.txn_ids) for c in cycles)

    print(f"Processed {len(labels):,} transactions in {elapsed:.0f}s")
    print(f"Detector stats: {dict(detector.stats)}")
    print(f"\nCycles found: {len(cycles):,}")
    if cycles:
        print(f"  all hops laundering: {pure:,} ({pure / len(cycles):.1%})")
        print(f"  any hop laundering:  {touched:,} ({touched / len(cycles):.1%})")
        print(f"  closed at tree root: {sum(c.closed_at_root for c in cycles):,}")
        print(f"  length distribution: {sorted(Counter(len(c.txn_ids) for c in cycles).items())}")

    print(f"\nFlagged accounts: {len(flagged):,}")
    if flagged:
        hit = len(flagged & laundering_accounts)
        print(f"  in a laundering transaction: {hit:,} ({hit / len(flagged):.1%})")
        print(
            f"  laundering accounts covered: {hit:,} / {len(laundering_accounts):,} "
            f"({hit / len(laundering_accounts):.1%})"
        )

    patterns = load_patterns(args.patterns)
    cycle_sets = [set(c.txn_ids) for c in cycles]
    by_txn = defaultdict(list)
    for i, s in enumerate(cycle_sets):
        for t in s:
            by_txn[t].append(i)

    print("\nPattern coverage (cycle fully inside pattern / cycle shares a hop):")
    totals = defaultdict(lambda: [0, 0, 0])
    unmatched = 0
    for kind, keys in patterns:
        txns = {key_to_txn[k] for k in keys if k in key_to_txn}
        unmatched += len(keys) - len(txns)
        candidates = {i for t in txns for i in by_txn.get(t, [])}
        totals[kind][0] += 1
        totals[kind][1] += any(cycle_sets[i] <= txns for i in candidates)
        totals[kind][2] += bool(candidates)
    for kind, (n, inside, shared) in sorted(totals.items()):
        print(f"  {kind:15} {inside:3}/{n:<3} ({inside / n:5.1%})   {shared:3}/{n:<3} ({shared / n:5.1%})")
    if unmatched:
        print(f"  ({unmatched} pattern rows could not be matched to a transaction)")


if __name__ == "__main__":
    main()
