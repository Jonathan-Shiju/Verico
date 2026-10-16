import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from adapters.streamdb import KafkaStreamConsumer, KafkaStreamProducer  # noqa: E402
from GraphIngest.Accounts import load_account_registry  # noqa: E402
from GraphIngest.GraphIngestor import GraphIngestor, IngestConfig  # noqa: E402
from GraphIngest.WindowFlags import WindowFlagger  # noqa: E402
from GraphIngest.Windows import WindowSpec  # noqa: E402
from scripts.cli_utils import parse_duration  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume transactions from Kafka into rolling FalkorDB window graphs."
    )
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--group", default="aml-graph-ingest")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--falkor-host", default="localhost")
    parser.add_argument("--falkor-port", type=int, default=6380)
    parser.add_argument("--window", type=parse_duration, default=parse_duration("8d"))
    parser.add_argument("--step", type=parse_duration, default=parse_duration("4d"))
    parser.add_argument("--graph-prefix", default="aml:txn")
    parser.add_argument(
        "--accounts",
        type=Path,
        default=PROJECT_ROOT / "transactions_files" / "HI-Small_accounts.csv",
        help="Account registry loaded into each window before any transaction",
    )
    parser.add_argument(
        "--retention",
        type=parse_duration,
        default=None,
        help="Delete closed windows this long (event time) after they close",
    )
    parser.add_argument("--target-write-seconds", type=float, default=0.5)
    parser.add_argument(
        "--idle-exit",
        type=float,
        default=None,
        help="Exit after this many seconds without messages (default: run forever)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    accounts = load_account_registry(args.accounts) if args.accounts.exists() else {}
    logging.info("Account registry: %s accounts", f"{len(accounts):,}")

    graph_db = FalkorGraphDB(args.falkor_host, args.falkor_port)
    consumer = KafkaStreamConsumer(
        args.bootstrap_servers,
        args.topic,
        args.group,
        auto_offset_reset="earliest",
        auto_commit=False,
    )
    ingestor = GraphIngestor(
        consumer,
        graph_db,
        WindowSpec(int(args.window), int(args.step), args.graph_prefix),
        dlq_producer=KafkaStreamProducer(args.bootstrap_servers),
        dlq_topic=f"{args.topic}.dlq",
        config=IngestConfig(
            target_write_seconds=args.target_write_seconds,
            window_retention_seconds=int(args.retention) if args.retention else None,
        ),
        accounts=accounts,
        flagger=WindowFlagger(graph_db),
    )
    try:
        stats = ingestor.run(idle_exit_seconds=args.idle_exit)
        logging.info("Stopped: %s", stats)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
