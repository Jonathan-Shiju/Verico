from collections import defaultdict
from dataclasses import dataclass
import json
import logging
import time

import redis

from adapters.graphdb import GraphDB
from adapters.streamdb import StreamConsumer, StreamProducer, StreamRecord
from GraphIngest.Accounts import account_id
from GraphIngest.Windows import WindowSpec, format_ts, global_watermark, parse_ts

logger = logging.getLogger(__name__)

INDEXES = [
    "CREATE INDEX FOR (a:Account) ON (a.id)",
    "CREATE INDEX FOR (a:Account) ON (a.entity_id)",
    "CREATE INDEX FOR ()-[t:TRANSFER]-() ON (t.txn_id)",
    "CREATE INDEX FOR ()-[t:TRANSFER]-() ON (t.ts)",
]

# FalkorDB's MERGE of entities created in the same query degrades quadratically
# past ~1000 rows, so writes are split into queries of at most this many rows.
QUERY_CHUNK = 1000

WRITE_ACCOUNTS = """
UNWIND $accounts AS n
MERGE (a:Account {id: n.id})
  ON CREATE SET a.bank = n.bank,
                a.bank_name = n.bank_name,
                a.entity_id = n.entity_id,
                a.entity_name = n.entity_name
"""

# MERGE on txn_id makes redelivered or retried rows no-ops.
WRITE_TRANSFERS = """
UNWIND $rows AS r
MATCH (a:Account {id: r.src}), (b:Account {id: r.dst})
MERGE (a)-[t:TRANSFER {txn_id: r.txn_id}]->(b)
  ON CREATE SET t.ts = r.ts,
                t.amount_paid = r.amount_paid,
                t.payment_currency = r.payment_currency,
                t.amount_received = r.amount_received,
                t.receiving_currency = r.receiving_currency,
                t.payment_format = r.payment_format
"""

TRANSIENT_ERRORS = (redis.ConnectionError, redis.TimeoutError)


@dataclass
class IngestConfig:
    initial_batch: int = 5_000
    min_batch: int = 200
    max_batch: int = 50_000
    target_write_seconds: float = 0.5
    poll_timeout_ms: int = 500
    max_retries: int = 5
    retry_backoff_seconds: float = 0.5
    window_retention_seconds: int | None = None
    log_every_seconds: float = 5.0


def to_row(value: dict) -> dict:
    """Raises KeyError/TypeError/ValueError for malformed transactions."""
    return {
        "txn_id": int(value["txn_id"]),
        "ts": parse_ts(value["timestamp"]),
        "src": account_id(value["from_bank"], value["from_account"]),
        "dst": account_id(value["to_bank"], value["to_account"]),
        "src_bank": str(int(value["from_bank"])),
        "dst_bank": str(int(value["to_bank"])),
        "amount_paid": float(value["amount_paid"]),
        "payment_currency": str(value["payment_currency"]),
        "amount_received": float(value["amount_received"]),
        "receiving_currency": str(value["receiving_currency"]),
        "payment_format": str(value["payment_format"]),
    }


class GraphIngestor:
    """Best-effort Kafka -> FalkorDB writer. Whatever does not fit in a cycle stays
    in Kafka (offsets are committed only after a successful write) and the
    published watermark tells downstream readers which event time is complete."""

    def __init__(
        self,
        consumer: StreamConsumer,
        graph_db: GraphDB,
        windows: WindowSpec,
        dlq_producer: StreamProducer | None = None,
        dlq_topic: str | None = None,
        config: IngestConfig | None = None,
        accounts: dict[str, dict] | None = None,
        flagger=None,
    ):
        self.consumer = consumer
        self.graph_db = graph_db
        self.windows = windows
        self.dlq_producer = dlq_producer
        self.dlq_topic = dlq_topic
        self.config = config or IngestConfig()
        self.accounts = accounts or {}
        self.flagger = flagger
        self.batch_size = self.config.initial_batch
        self._ready_graphs: set[str] = set()
        self._registered: dict[str, set[str]] = {}
        self.stats = defaultdict(int)

    def run(self, idle_exit_seconds: float | None = None) -> dict:
        partitions = self.consumer.partition_count()
        last_data = time.monotonic()
        next_log = last_data + self.config.log_every_seconds

        while True:
            records = self.consumer.consume_batch(
                self.batch_size, self.config.poll_timeout_ms
            )
            now = time.monotonic()
            caught_up = not records and self.consumer.exhausted()
            if records:
                last_data = now
                self._process(records)
            watermark = global_watermark(self.graph_db, self.windows, partitions, caught_up)
            self._close_windows(watermark)
            if (
                not records
                and idle_exit_seconds is not None
                and now - last_data >= idle_exit_seconds
            ):
                break

            if now >= next_log:
                logger.info(
                    "written=%s dead=%s batch=%d last_write=%.2fs watermark=%s",
                    f"{self.stats['written']:,}",
                    self.stats["dead_lettered"],
                    self.batch_size,
                    self.stats["last_write_ms"] / 1000,
                    format_ts(watermark) if watermark is not None else "-",
                )
                next_log = now + self.config.log_every_seconds

        if self.dlq_producer:
            self.dlq_producer.flush()
        return dict(self.stats)

    def _process(self, records: list[StreamRecord]) -> None:
        rows_by_window: dict[int, list[dict]] = defaultdict(list)
        latest_ts: dict[int, int] = {}
        dead: dict[int, tuple[dict, str]] = {}

        for record in records:
            if record.error or record.value is None:
                self._dead_letter_raw(record, record.error or "empty message")
                continue
            try:
                row = to_row(record.value)
            except (KeyError, TypeError, ValueError) as err:
                self._dead_letter_raw(record, f"malformed transaction: {err!r}")
                continue
            for start in self.windows.starts_for(row["ts"]):
                rows_by_window[start].append(row)
            latest_ts[record.partition] = max(latest_ts.get(record.partition, 0), row["ts"])

        started = time.monotonic()
        for start, rows in rows_by_window.items():
            graph = self.windows.graph_name(start)
            self._ensure_graph(graph, start)
            failed: list[tuple[dict, str]] = []
            for i in range(0, len(rows), QUERY_CHUNK):
                self._write(graph, rows[i : i + QUERY_CHUNK], failed)
            for row, err in failed:
                dead.setdefault(row["txn_id"], (row, err))
            if self.flagger is not None and rows:
                accounts = list({a for row in rows for a in (row["src"], row["dst"])})
                try:
                    self.stats["flags"] += self.flagger.check(graph, accounts)
                except Exception:
                    logger.exception("Flag check failed for %s", graph)
        elapsed = time.monotonic() - started

        for row, err in dead.values():
            self._dead_letter(row["txn_id"], row, err)
        self.stats["written"] += len(records) - len(dead)
        self.stats["last_write_ms"] = int(elapsed * 1000)
        self._adapt_batch(elapsed)

        marks = self.graph_db.get_state(self.windows.watermark_key)
        for partition, ts in latest_ts.items():
            if ts > int(marks.get(str(partition), -1)):
                self.graph_db.set_state(
                    self.windows.watermark_key, str(partition), str(ts)
                )
        self.consumer.commit(records)

    def _write(self, graph: str, rows: list[dict], failed: list) -> None:
        try:
            self._write_with_retry(graph, rows)
        except redis.ResponseError as err:
            if len(rows) == 1:
                failed.append((rows[0], str(err)))
                return
            self.stats["bisections"] += 1
            mid = len(rows) // 2
            self._write(graph, rows[:mid], failed)
            self._write(graph, rows[mid:], failed)

    def _write_with_retry(self, graph: str, rows: list[dict]) -> None:
        # Registered accounts were created when the window opened. Only an account
        # that shows up in a transaction without being registered is created here.
        known = self._registered.setdefault(graph, set())
        missing = []
        for row in rows:
            for acct, bank in ((row["src"], row["src_bank"]), (row["dst"], row["dst_bank"])):
                if acct not in known:
                    missing.append(
                        {
                            "id": acct,
                            "bank": bank,
                            "bank_name": "",
                            "entity_id": "",
                            "entity_name": "",
                        }
                    )
                    known.add(acct)
        new_ids = [account["id"] for account in missing]
        for attempt in range(self.config.max_retries + 1):
            try:
                for i in range(0, len(missing), QUERY_CHUNK):
                    self.graph_db.query(
                        graph, WRITE_ACCOUNTS, {"accounts": missing[i : i + QUERY_CHUNK]}
                    )
                self.graph_db.query(graph, WRITE_TRANSFERS, {"rows": rows})
                return
            except TRANSIENT_ERRORS:
                if attempt == self.config.max_retries:
                    known.difference_update(new_ids)
                    raise
                self.stats["retries"] += 1
                time.sleep(self.config.retry_backoff_seconds * 2**attempt)

    def _adapt_batch(self, elapsed: float) -> None:
        target = self.config.target_write_seconds
        if elapsed > target:
            self.batch_size = max(self.config.min_batch, self.batch_size // 2)
        elif elapsed < target / 2:
            self.batch_size = min(
                self.config.max_batch, self.batch_size + self.config.initial_batch // 2
            )

    def _ensure_graph(self, graph: str, start: int) -> None:
        if graph in self._ready_graphs:
            return
        self.graph_db.ensure_indexes(graph, INDEXES)
        state = self.graph_db.get_state(self.windows.windows_key)
        window = json.loads(state[graph]) if graph in state else {
            "start": start,
            "end": start + self.windows.length_seconds,
            "status": "open",
        }
        if self.accounts and not window.get("accounts_loaded"):
            self._load_registered_accounts(graph)
            window["accounts_loaded"] = True
        self.graph_db.set_state(self.windows.windows_key, graph, json.dumps(window))
        self._registered[graph] = set(self.accounts)
        self._ready_graphs.add(graph)

    def _load_registered_accounts(self, graph: str) -> None:
        rows = list(self.accounts.values())
        started = time.monotonic()
        for i in range(0, len(rows), QUERY_CHUNK):
            self.graph_db.query(graph, WRITE_ACCOUNTS, {"accounts": rows[i : i + QUERY_CHUNK]})
        logger.info(
            "Registered %s accounts in %s in %.1fs",
            f"{len(rows):,}",
            graph,
            time.monotonic() - started,
        )

    def _close_windows(self, watermark: int | None) -> None:
        if watermark is None:
            return
        retention = self.config.window_retention_seconds
        for graph, raw in self.graph_db.get_state(self.windows.windows_key).items():
            window = json.loads(raw)
            if window["status"] == "open" and window["end"] <= watermark:
                window["status"] = "closed"
                logger.info("Window %s closed", graph)
            elif (
                window["status"] == "closed"
                and retention is not None
                and window["end"] + retention <= watermark
            ):
                self.graph_db.delete_graph(graph)
                window["status"] = "deleted"
                logger.info("Window %s deleted", graph)
            else:
                continue
            self.graph_db.set_state(self.windows.windows_key, graph, json.dumps(window))

    def _dead_letter_raw(self, record: StreamRecord, error: str) -> None:
        raw = record.raw.decode("utf-8", errors="replace") if record.raw else None
        self._dead_letter(record.key, {"raw": raw}, error)

    def _dead_letter(self, key, payload: dict, error: str) -> None:
        self.stats["dead_lettered"] += 1
        logger.warning("Dead-lettering %s: %s", key, error)
        if self.dlq_producer and self.dlq_topic:
            self.dlq_producer.async_send(
                self.dlq_topic,
                key=str(key) if key is not None else None,
                value={"error": error, "payload": payload},
            )
