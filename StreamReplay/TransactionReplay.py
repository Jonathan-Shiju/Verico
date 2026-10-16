from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from itertools import groupby
from pathlib import Path
import csv
import logging
import time

from adapters.staticdb import StaticDB
from adapters.streamdb import StreamProducer

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"
# Rows start with "YYYY/MM/DD HH:MM", so byte prefixes sort chronologically.
MINUTE_PREFIX = 16
HOUR_PREFIX = 13


def _bucket_key(dataset_id: str, hour: str) -> str:
    return f"{dataset_id}:{hour.replace('/', '').replace(' ', 'T')}"


def _to_message(fields: list[str]) -> dict:
    return {
        "txn_id": int(fields[11]),
        "timestamp": fields[0],
        "from_bank": fields[1],
        "from_account": fields[2],
        "to_bank": fields[3],
        "to_account": fields[4],
        "amount_received": float(fields[5]),
        "receiving_currency": fields[6],
        "amount_paid": float(fields[7]),
        "payment_currency": fields[8],
        "payment_format": fields[9],
        "is_laundering": int(fields[10]),
    }


class TransactionStore:
    """Stores an IBM AML transactions CSV as hourly buckets so it can be read back
    in time order without loading the whole (unsorted) file into memory."""

    def __init__(
        self,
        static_db: StaticDB,
        chunk_bytes: int = 4 << 20,
        max_buffer_bytes: int = 256 << 20,
    ):
        self.static_db = static_db
        self.chunk_bytes = chunk_bytes
        self.max_buffer_bytes = max_buffer_bytes

    def load_csv(self, path: Path, dataset_id: str) -> dict:
        self.delete(dataset_id)

        buffers: dict[bytes, list[bytes]] = defaultdict(list)
        sizes: dict[bytes, int] = defaultdict(int)
        buckets: set[bytes] = set()
        buffered = rows = 0
        start = end = None

        def flush(hour: bytes) -> None:
            nonlocal buffered
            self.static_db.append_chunk(
                _bucket_key(dataset_id, hour.decode()), b"".join(buffers.pop(hour))
            )
            buffered -= sizes.pop(hour)

        with open(path, "rb") as f:
            header = f.readline().decode().strip()
            for line in f:
                line = line.rstrip(b"\r\n")
                if not line:
                    continue
                line += b",%d\n" % rows
                minute = line[:MINUTE_PREFIX]
                hour = line[:HOUR_PREFIX]
                if start is None or minute < start:
                    start = minute
                if end is None or minute > end:
                    end = minute

                buffers[hour].append(line)
                sizes[hour] += len(line)
                buffered += len(line)
                buckets.add(hour)
                rows += 1

                if sizes[hour] >= self.chunk_bytes:
                    flush(hour)
                elif buffered >= self.max_buffer_bytes:
                    for pending in list(buffers):
                        flush(pending)

        for pending in list(buffers):
            flush(pending)

        metadata = {
            "filename": Path(path).name,
            "header": f"{header},txn_id",
            "rows": rows,
            "start": start.decode() if start else None,
            "end": end.decode() if end else None,
            "buckets": sorted(hour.decode() for hour in buckets),
        }
        self.static_db.save_metadata(dataset_id, metadata)
        return metadata

    def get_metadata(self, dataset_id: str) -> dict:
        metadata = self.static_db.get_metadata(dataset_id)
        if "buckets" not in metadata:
            raise FileNotFoundError(f"Transaction dataset {dataset_id} not found.")
        return metadata

    def iter_minutes(self, dataset_id: str) -> Iterator[tuple[datetime, list[dict]]]:
        """Yields (timestamp, transactions) for every minute that has data, in order."""
        for hour in self.get_metadata(dataset_id)["buckets"]:
            lines = []
            for chunk in self.static_db.iter_chunks(_bucket_key(dataset_id, hour)):
                lines.extend(chunk.splitlines())
            lines.sort(key=lambda line: line[:MINUTE_PREFIX])

            for minute, group in groupby(lines, key=lambda line: line[:MINUTE_PREFIX]):
                timestamp = datetime.strptime(minute.decode(), TIMESTAMP_FORMAT)
                rows = csv.reader(line.decode() for line in group)
                yield timestamp, [_to_message(fields) for fields in rows]

    def delete(self, dataset_id: str) -> None:
        for hour in self.static_db.get_metadata(dataset_id).get("buckets", []):
            self.static_db.delete(_bucket_key(dataset_id, hour))
        self.static_db.delete(dataset_id)


class TransactionReplayer:
    """Replays a stored dataset onto a stream, compressing its original time span
    into `duration_seconds` of wall-clock time while keeping relative spacing."""

    def __init__(
        self,
        store: TransactionStore,
        stream_producer: StreamProducer,
        topic: str,
        log_every_seconds: float = 5.0,
    ):
        self.store = store
        self.stream_producer = stream_producer
        self.topic = topic
        self.log_every_seconds = log_every_seconds

    def replay(self, dataset_id: str, duration_seconds: float) -> dict:
        metadata = self.store.get_metadata(dataset_id)
        start = datetime.strptime(metadata["start"], TIMESTAMP_FORMAT)
        end = datetime.strptime(metadata["end"], TIMESTAMP_FORMAT)
        span = (end - start).total_seconds()
        scale = duration_seconds / span if span else 0.0
        logger.info(
            "Replaying %s rows (%s -> %s) in %.0fs, %.0fx speed-up",
            f"{metadata['rows']:,}",
            start,
            end,
            duration_seconds,
            1 / scale if scale else float("inf"),
        )

        wall_start = time.monotonic()
        next_log = wall_start + self.log_every_seconds
        sent = 0
        max_lag = 0.0

        for timestamp, transactions in self.store.iter_minutes(dataset_id):
            due = wall_start + (timestamp - start).total_seconds() * scale
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            else:
                max_lag = max(max_lag, -delay)

            for transaction in transactions:
                self.stream_producer.async_send(
                    self.topic, key=transaction["from_account"], value=transaction
                )
            sent += len(transactions)

            now = time.monotonic()
            if now >= next_log:
                logger.info(
                    "sent=%s simulated=%s elapsed=%.1fs lag=%.2fs",
                    f"{sent:,}",
                    timestamp,
                    now - wall_start,
                    max(0.0, -delay),
                )
                next_log = now + self.log_every_seconds

        self.stream_producer.flush()
        elapsed = time.monotonic() - wall_start
        logger.info(
            "Done: sent=%s in %.1fs (max lag %.2fs)", f"{sent:,}", elapsed, max_lag
        )
        return {"sent": sent, "elapsed_seconds": elapsed, "max_lag_seconds": max_lag}
