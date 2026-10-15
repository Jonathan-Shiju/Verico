from abc import ABC, abstractmethod
from collections.abc import Iterator
import json
import zlib
import redis


class StaticDB(ABC):
    @abstractmethod
    def save(self, file_id: str, data: bytes, metadata: dict) -> None:
        pass

    @abstractmethod
    def get(self, file_id: str) -> tuple[bytes, dict]:
        pass

    @abstractmethod
    def delete(self, file_id: str) -> bool:
        pass

    @abstractmethod
    def append_chunk(self, file_id: str, chunk: bytes) -> None:
        pass

    @abstractmethod
    def iter_chunks(self, file_id: str) -> Iterator[bytes]:
        pass

    @abstractmethod
    def save_metadata(self, file_id: str, metadata: dict) -> None:
        pass

    @abstractmethod
    def get_metadata(self, file_id: str) -> dict:
        pass


class Redis(StaticDB):
    # Huge Chunks must be compressed to not exceed the limit of Redis
    COMPRESSION_LEVEL = 3
    FETCH_BATCH = 8

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        self.client = redis.StrictRedis(host=host, port=port, db=db)

    def save(self, file_id: str, data: bytes, metadata: dict) -> None:
        self.client.set(file_id, data)
        self.save_metadata(file_id, metadata)

    def get(self, file_id: str) -> tuple[bytes, dict]:
        data = self.client.get(file_id)
        if data is None and self.client.exists(f"{file_id}:chunks"):
            data = b"".join(self.iter_chunks(file_id))
        return data, self.get_metadata(file_id)

    def delete(self, file_id: str) -> bool:
        data_deleted = self.client.delete(file_id, f"{file_id}:chunks")
        metadata_deleted = self.client.delete(f"{file_id}:metadata")
        return data_deleted > 0 and metadata_deleted > 0

    def append_chunk(self, file_id: str, chunk: bytes) -> None:
        self.client.rpush(
            f"{file_id}:chunks", zlib.compress(chunk, self.COMPRESSION_LEVEL)
        )

    # Iterate over chunks in batched with size FETCH_BATCH
    def iter_chunks(self, file_id: str) -> Iterator[bytes]:
        key = f"{file_id}:chunks"
        start = 0
        while True:
            batch = self.client.lrange(key, start, start + self.FETCH_BATCH - 1)
            if not batch:
                return
            for chunk in batch:
                yield zlib.decompress(chunk)
            start += len(batch)

    def save_metadata(self, file_id: str, metadata: dict) -> None:
        self.client.set(f"{file_id}:metadata", json.dumps(metadata))

    def get_metadata(self, file_id: str) -> dict:
        metadata_json = self.client.get(f"{file_id}:metadata")
        return json.loads(metadata_json) if metadata_json else {}
