import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.staticdb import Redis  # noqa: E402
from adapters.streamdb import KafkaStreamProducer, ensure_topic  # noqa: E402
from scripts.cli_utils import parse_duration  # noqa: E402
from StreamReplay.TransactionReplay import (  # noqa: E402
    TransactionReplayer,
    TransactionStore,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay a stored transactions dataset onto Kafka in compressed time."
    )
    parser.add_argument("--dataset", default="HI-Small_Trans")
    parser.add_argument(
        "--duration",
        type=parse_duration,
        default=parse_duration("10m"),
        help="Wall-clock time to replay the whole dataset in (e.g. 90s, 10m, 1h30m)",
    )
    parser.add_argument("--topic", default="transactions")
    parser.add_argument(
        "--partitions",
        type=int,
        default=6,
        help="Partition count used if the topic has to be created",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    partitions = ensure_topic(args.bootstrap_servers, args.topic, args.partitions)
    logging.info("Topic %s has %d partitions", args.topic, partitions)

    connector = Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    replayer = TransactionReplayer(
        TransactionStore(connector),
        KafkaStreamProducer(args.bootstrap_servers),
        args.topic,
    )
    replayer.replay(args.dataset, args.duration)


if __name__ == "__main__":
    main()
