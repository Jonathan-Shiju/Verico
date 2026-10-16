from collections import Counter, deque
from dataclasses import dataclass, field


@dataclass
class TreeConfig:
    """All bounds are business or compute limits, not dataset-specific values."""

    # Business lookback: a tree stops growing this long after its root transaction.
    horizon_seconds: int = 30 * 86400
    # Compute bounds on a single tree.
    max_depth: int = 10
    max_expansions_per_root: int = 512
    # Memory bound: open leaves kept per account (oldest dropped first).
    max_leaves_per_account: int = 128
    # Branching bound: payments followed from one leaf before it closes.
    max_children_per_leaf: int = 8
    # A payment can continue a leaf if its value is within
    # [held * min_flow_fraction, held * (1 + amount_tolerance)]; the upper slack
    # absorbs FX conversion and fees along a hop.
    amount_tolerance: float = 0.25
    min_flow_fraction: float = 0.05
    # Accounts paying more than this many unique recipients are not expanded.
    spread_limit: int = 100
    # Self-transfers with these formats are ignored for leaves whose depth lies in
    # [self_skip_min_depth, self_skip_max_depth] (None = no upper bound).
    excluded_self_formats: frozenset[str] = frozenset({"Reinvestment"})
    self_skip_min_depth: int = 1
    self_skip_max_depth: int | None = None
    # How many transactions between sweeps of expired state.
    sweep_every: int = 200_000


@dataclass
class Cycle:
    accounts: tuple[str, ...]
    txn_ids: tuple[int, ...]
    start_ts: int
    end_ts: int
    amount_out: float
    amount_back: float
    tree_root: str
    closed_at_root: bool
    # False for a cycle. True for a chain of at least two hops that never paid
    # back into an account already on its path.
    open: bool = False


@dataclass(slots=True)
class _Root:
    account: str
    ts: int
    expansions: int = 0


@dataclass(slots=True)
class _Leaf:
    root: _Root
    account: str
    arrival_ts: int
    held: float
    currency: str
    depth: int
    path: tuple[str, ...]
    # (txn_id, ts, amount_paid) per hop
    hops: tuple[tuple[int, int, float], ...]
    children: int = 0


@dataclass
class TreeLoopDetector:
    """Grows a time-respecting money-flow tree from every transaction and reports a
    cycle whenever a branch pays back into an account already on its path.

    Transactions must be fed in non-decreasing event-time order."""

    config: TreeConfig = field(default_factory=TreeConfig)

    def __post_init__(self):
        self.leaves: dict[str, deque[_Leaf]] = {}
        self.recipients: dict[str, set[str]] = {}
        self.hubs: set[str] = set()
        self.fx: dict[tuple[str, str], float] = {}
        self.reported: dict[tuple[int, ...], int] = {}
        self.stats: Counter = Counter()
        self._since_sweep = 0
        # Unfinished chains (two or more hops) dropped before they could close.
        self._open: dict[tuple[int, ...], Cycle] = {}

    def process(self, txn: dict) -> list[Cycle]:
        src, dst = txn["src"], txn["dst"]
        self.stats["transactions"] += 1
        self._learn_fx(txn)
        if src != dst:
            self._track_spread(src, dst)

        cycles: list[Cycle] = []
        if src in self.leaves:
            self._extend(src, txn, cycles)
        if src != dst and src not in self.hubs:
            self._seed(txn)

        self._since_sweep += 1
        if self._since_sweep >= self.config.sweep_every:
            self.sweep(txn["ts"])
        return cycles

    def sweep(self, now: int) -> None:
        horizon = self.config.horizon_seconds
        for account in list(self.leaves):
            alive = []
            for leaf in self.leaves[account]:
                if now - leaf.root.ts <= horizon:
                    alive.append(leaf)
                else:
                    self._note_open(leaf)
            if alive:
                self.leaves[account] = deque(alive, maxlen=self.config.max_leaves_per_account)
            else:
                del self.leaves[account]
        self.reported = {k: ts for k, ts in self.reported.items() if now - ts <= horizon}
        self._since_sweep = 0

    def open_leaves(self) -> int:
        return sum(len(q) for q in self.leaves.values())

    def finish(self) -> list[Cycle]:
        """Maximal unfinished chains. A one-hop seed is not a chain. A path that
        is a prefix of a longer open path is not its own lead."""
        for queue in self.leaves.values():
            for leaf in queue:
                self._note_open(leaf)
        self.leaves.clear()
        return _maximal(list(self._open.values()))

    def _learn_fx(self, txn: dict) -> None:
        paid, received = txn["amount_paid"], txn["amount_received"]
        pay_cur, recv_cur = txn["payment_currency"], txn["receiving_currency"]
        if pay_cur != recv_cur and paid > 0 and received > 0:
            self.fx[(pay_cur, recv_cur)] = received / paid
            self.fx[(recv_cur, pay_cur)] = paid / received

    def _convert(self, amount: float, from_cur: str, to_cur: str) -> float | None:
        if from_cur == to_cur:
            return amount
        rate = self.fx.get((from_cur, to_cur))
        return amount * rate if rate is not None else None

    def _track_spread(self, src: str, dst: str) -> None:
        if src in self.hubs:
            return
        seen = self.recipients.setdefault(src, set())
        seen.add(dst)
        if len(seen) > self.config.spread_limit:
            self.hubs.add(src)
            del self.recipients[src]
            self.leaves.pop(src, None)
            self.stats["hubs"] += 1

    def _seed(self, txn: dict) -> None:
        root = _Root(txn["src"], txn["ts"])
        self._add_leaf(
            _Leaf(
                root=root,
                account=txn["dst"],
                arrival_ts=txn["ts"],
                held=txn["amount_received"],
                currency=txn["receiving_currency"],
                depth=1,
                path=(txn["src"], txn["dst"]),
                hops=((txn["txn_id"], txn["ts"], txn["amount_paid"]),),
            )
        )

    def _add_leaf(self, leaf: _Leaf) -> None:
        if leaf.depth >= self.config.max_depth or leaf.account in self.hubs:
            self.stats["leaves_terminated"] += 1
            if leaf.account not in self.hubs:
                self._note_open(leaf)
            return
        queue = self.leaves.get(leaf.account)
        if queue is None:
            queue = self.leaves[leaf.account] = deque(
                maxlen=self.config.max_leaves_per_account
            )
        elif len(queue) == queue.maxlen:
            self._note_open(queue[0])
            self.stats["leaves_evicted"] += 1
        queue.append(leaf)

    def _extend(self, account: str, txn: dict, cycles: list[Cycle]) -> None:
        cfg = self.config
        ts, dst = txn["ts"], txn["dst"]
        is_self = txn["src"] == dst
        skip_self = is_self and txn["payment_format"] in cfg.excluded_self_formats
        hop = (txn["txn_id"], ts, txn["amount_paid"])

        queue = self.leaves[account]
        removed = False
        spawned: list[_Leaf] = []
        for leaf in queue:
            if ts - leaf.root.ts > cfg.horizon_seconds or leaf.children >= cfg.max_children_per_leaf:
                removed = True
                continue
            if leaf.arrival_ts > ts:
                continue
            if skip_self and leaf.depth >= cfg.self_skip_min_depth and (
                cfg.self_skip_max_depth is None or leaf.depth <= cfg.self_skip_max_depth
            ):
                continue

            spent = self._convert(txn["amount_paid"], txn["payment_currency"], leaf.currency)
            if spent is not None and not (
                leaf.held * cfg.min_flow_fraction
                <= spent
                <= leaf.held * (1 + cfg.amount_tolerance)
            ):
                continue

            if dst in leaf.path and not is_self:
                self._report(leaf, dst, txn, hop, cycles)
            elif leaf.root.expansions < cfg.max_expansions_per_root:
                leaf.root.expansions += 1
                leaf.children += 1
                spawned.append(
                    _Leaf(
                        root=leaf.root,
                        account=dst,
                        arrival_ts=ts,
                        held=txn["amount_received"],
                        currency=txn["receiving_currency"],
                        depth=leaf.depth + 1,
                        path=leaf.path + (dst,),
                        hops=leaf.hops + (hop,),
                    )
                )
            else:
                self.stats["budget_exhausted"] += 1

        if removed:
            survivors = []
            for leaf in queue:
                if (
                    ts - leaf.root.ts <= cfg.horizon_seconds
                    and leaf.children < cfg.max_children_per_leaf
                ):
                    survivors.append(leaf)
                else:
                    self._note_open(leaf)
            if survivors:
                self.leaves[account] = deque(survivors, maxlen=cfg.max_leaves_per_account)
            else:
                del self.leaves[account]
        for leaf in spawned:
            self._add_leaf(leaf)
        self.stats["leaves_spawned"] += len(spawned)

    def _report(self, leaf: _Leaf, dst: str, txn: dict, hop, cycles: list[Cycle]) -> None:
        start = leaf.path.index(dst)
        hops = leaf.hops[start:] + (hop,)
        key = tuple(h[0] for h in hops)
        if key in self.reported:
            return
        for end in range(1, len(key) + 1):
            prefix = key[:end]
            self.reported[prefix] = txn["ts"]
            self._open.pop(prefix, None)
        self.stats["cycles"] += 1
        cycles.append(
            Cycle(
                accounts=leaf.path[start:],
                txn_ids=key,
                start_ts=hops[0][1],
                end_ts=txn["ts"],
                amount_out=hops[0][2],
                amount_back=txn["amount_received"],
                tree_root=leaf.root.account,
                closed_at_root=start == 0,
            )
        )

    def _note_open(self, leaf: _Leaf) -> None:
        """Remember a chain of at least two hops. One hop is just a payment."""
        if len(leaf.hops) < 2:
            return
        key = tuple(hop[0] for hop in leaf.hops)
        if key in self.reported or key in self._open:
            return
        self._open[key] = Cycle(
            accounts=leaf.path,
            txn_ids=key,
            start_ts=leaf.hops[0][1],
            end_ts=leaf.hops[-1][1],
            amount_out=leaf.hops[0][2],
            amount_back=leaf.held,
            tree_root=leaf.root.account,
            closed_at_root=False,
            open=True,
        )
        self.stats["open_chains"] += 1


def _maximal(chains: list[Cycle]) -> list[Cycle]:
    """Drop a chain whose transactions sit inside a longer chain."""
    covered: set[tuple[int, ...]] = set()
    kept = []
    ordered = sorted(chains, key=lambda chain: len(chain.txn_ids), reverse=True)
    for chain in ordered:
        if chain.txn_ids in covered:
            continue
        kept.append(chain)
        ids = chain.txn_ids
        for start in range(len(ids)):
            for end in range(start + 1, len(ids) + 1):
                covered.add(ids[start:end])
    return kept
