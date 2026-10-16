import argparse
import mimetypes
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from adapters.staticdb import Redis, StaticDB  # noqa: E402
from StreamReplay.TransactionReplay import TransactionStore  # noqa: E402

DEFAULT_DIR = PROJECT_ROOT / "transactions_files"
CHUNK_BYTES = 4 << 20


def save_file_in_chunks(db: StaticDB, path: Path, file_id: str) -> int:
    db.delete(file_id)
    size = 0
    with open(path, "rb") as f:
        while lines := f.readlines(CHUNK_BYTES):
            chunk = b"".join(lines)
            db.append_chunk(file_id, chunk)
            size += len(chunk)
    db.save_metadata(
        file_id,
        {
            "filename": path.name,
            "size_bytes": size,
            "content_type": mimetypes.guess_type(path.name)[0] or "text/plain",
        },
    )
    return size


def load_files(db: StaticDB, directory: Path) -> list[str]:
    store = TransactionStore(db)
    saved = []
    for path in sorted(directory.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        started = time.monotonic()
        if path.stem.endswith("_Trans"):
            metadata = store.load_csv(path, path.stem)
            detail = (
                f"{metadata['rows']:,} rows, {metadata['start']} -> {metadata['end']}, "
                f"{len(metadata['buckets'])} hourly buckets"
            )
        else:
            detail = f"{save_file_in_chunks(db, path, path.stem):,} bytes"
        saved.append(path.stem)
        print(
            f"Saved {path.name} as '{path.stem}' ({detail}) "
            f"in {time.monotonic() - started:.1f}s"
        )
    return saved


def main() -> None:
    parser = argparse.ArgumentParser(description="Load transaction files into Redis.")
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--db", type=int, default=0)
    args = parser.parse_args()

    connector = Redis(host=args.host, port=args.port, db=args.db)
    connector.client.ping()
    load_files(connector, args.dir)


if __name__ == "__main__":
    main()
