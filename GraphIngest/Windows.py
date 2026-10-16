from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache

from adapters.graphdb import GraphDB

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


@lru_cache(maxsize=65536)
def parse_ts(timestamp: str) -> int:
    """Event timestamps are naive in the source data; they are treated as UTC."""
    parsed = datetime.strptime(timestamp, TIMESTAMP_FORMAT)
    return int(parsed.replace(tzinfo=timezone.utc).timestamp())


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime(TIMESTAMP_FORMAT)


@dataclass(frozen=True)
class WindowSpec:
    """Hopping windows aligned to the Unix epoch: a new window of `length_seconds`
    starts every `step_seconds`, so each event lands in length/step windows."""

    length_seconds: int
    step_seconds: int
    prefix: str = "aml:txn"

    def __post_init__(self):
        if self.step_seconds <= 0 or self.length_seconds < self.step_seconds:
            raise ValueError("Window length must be >= step and step must be > 0")

    def starts_for(self, ts: int) -> list[int]:
        start = ts // self.step_seconds * self.step_seconds
        starts = []
        while start > ts - self.length_seconds:
            starts.append(start)
            start -= self.step_seconds
        return starts

    def graph_name(self, start: int) -> str:
        stamp = datetime.fromtimestamp(start, timezone.utc).strftime("%Y%m%dT%H%M")
        return f"{self.prefix}:{stamp}"

    @property
    def watermark_key(self) -> str:
        return f"{self.prefix}:watermark"

    @property
    def windows_key(self) -> str:
        return f"{self.prefix}:windows"


def global_watermark(
    graph_db: GraphDB, spec: WindowSpec, partitions: int, caught_up: bool = False
) -> int | None:
    """Event time up to which every partition has been written.

    While any partition still has unread messages, this is the earliest partition
    mark: a later mark on another partition does not mean the slower one has
    reached it. Once every partition has been read to the end of the log, nothing
    remains in flight and the mark is the latest event across all partitions."""
    marks = graph_db.get_state(spec.watermark_key)
    values = [int(value) for key, value in marks.items() if key.isdigit()]
    if len(values) < partitions:
        return None
    watermark = max(values) if caught_up else min(values)
    graph_db.set_state(spec.watermark_key, "resolved", str(watermark))
    return watermark
