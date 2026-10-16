import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from adapters.streamdb import KafkaStreamConsumer  # noqa: E402
from Detection.LoopSink import EventTimeOrderer, FalkorLoopSink  # noqa: E402
from Detection.TreeLoopDetector import TreeConfig, TreeLoopDetector  # noqa: E402
from GraphIngest.GraphIngestor import to_row  # noqa: E402
from GraphIngest.Windows import format_ts  # noqa: E402
from scripts.cli_utils import parse_duration  # noqa: E402

logger = logging.getLogger("detect_loops")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect time-respecting money loops from the transaction stream."
    )
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--group", default="aml-loop-detector")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--falkor-host", default="localhost")
    parser.add_argument("--falkor-port", type=int, default=6380)
    parser.add_argument("--loop-graph", default="aml:loops")
    parser.add_argument("--horizon", type=parse_duration, default=parse_duration("30d"))
    parser.add_argument("--max-depth", type=int, default=10)
    parser.add_argument("--max-expansions-per-root", type=int, default=512)
    parser.add_argument("--max-leaves-per-account", type=int, default=128)
    parser.add_argument("--amount-tolerance", type=float, default=0.25)
    parser.add_argument("--min-flow-fraction", type=float, default=0.05)
    parser.add_argument("--spread-limit", type=int, default=100)
    parser.add_argument(
        "--exclude-self-formats",
        default="Reinvestment",
        help="Comma-separated payment formats whose self-transfers are ignored",
    )
    parser.add_argument("--self-skip-min-depth", type=int, default=1)
    parser.add_argument("--self-skip-max-depth", type=int, default=None)
    parser.add_argument("--idle-exit", type=float, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    config = TreeConfig(
        horizon_seconds=int(args.horizon),
        max_depth=args.max_depth,
        max_expansions_per_root=args.max_expansions_per_root,
        max_leaves_per_account=args.max_leaves_per_account,
        amount_tolerance=args.amount_tolerance,
        min_flow_fraction=args.min_flow_fraction,
        spread_limit=args.spread_limit,
        excluded_self_formats=frozenset(
            f.strip() for f in args.exclude_self_formats.split(",") if f.strip()
        ),
        self_skip_min_depth=args.self_skip_min_depth,
        self_skip_max_depth=args.self_skip_max_depth,
    )
    detector = TreeLoopDetector(config)
    graph_db = FalkorGraphDB(args.falkor_host, args.falkor_port)
    sink = FalkorLoopSink(graph_db, args.loop_graph)
    # Detector state lives in memory, so it always rebuilds from the start of the topic.
    consumer = KafkaStreamConsumer(
        args.bootstrap_servers,
        args.topic,
        f"{args.group}-{int(time.time())}",
        auto_offset_reset="earliest",
        auto_commit=False,
    )
    orderer = EventTimeOrderer(consumer.partition_count())

    def feed(rows: list[dict]) -> None:
        cycles = []
        for row in rows:
            cycles.extend(detector.process(row))
        if cycles:
            sink.write(cycles)
        if rows:
            graph_db.set_state(f"{args.loop_graph}:watermark", "ts", str(rows[-1]["ts"]))

    started = last_data = time.monotonic()
    next_log = started + 5
    try:
        while True:
            records = consumer.consume_batch(10_000, 500)
            now = time.monotonic()
            for record in records:
                if record.value is None:
                    continue
                try:
                    row = to_row(record.value)
                except (KeyError, TypeError, ValueError):
                    continue
                orderer.add(record.partition, row["ts"], row)
            if records:
                last_data = now
                feed(orderer.release())
            elif args.idle_exit is not None and now - last_data >= args.idle_exit:
                feed(orderer.drain())
                opened = detector.finish()
                if opened:
                    sink.write(opened)
                logger.info("open_chains=%s", f"{len(opened):,}")
                break
            if now >= next_log:
                low = orderer.low_watermark()
                logger.info(
                    "processed=%s cycles=%s open_leaves=%s hubs=%s event_time=%s",
                    f"{detector.stats['transactions']:,}",
                    f"{detector.stats['cycles']:,}",
                    f"{detector.open_leaves():,}",
                    f"{detector.stats['hubs']:,}",
                    format_ts(low) if low is not None else "-",
                )
                next_log = now + 5
    finally:
        consumer.close()
    logger.info("Done in %.0fs: %s", time.monotonic() - started, dict(detector.stats))


if __name__ == "__main__":
    main()
