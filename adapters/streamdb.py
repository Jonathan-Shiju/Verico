"""
To simulate transactions being streamed in real-time.
Interfaces for Producer and Consumer with Kafka implementation.
Topics have a fixed set of partitions, each partition is ordered and immutable.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic


class StreamProducer(ABC):
    @abstractmethod
    def async_send(self, topic: str, key: str, value: dict) -> None:
        pass

    @abstractmethod
    def sync_send(self, topic: str, key: str, value: dict) -> None:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass


@dataclass
class StreamRecord:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict | None
    raw: bytes | None
    error: str | None = None


class StreamConsumer(ABC):
    @abstractmethod
    def consume(self, timeout: int):
        pass

    @abstractmethod
    def consume_batch(self, max_records: int, timeout_ms: int) -> list[StreamRecord]:
        pass

    @abstractmethod
    def commit(self, records: list[StreamRecord]) -> None:
        pass

    @abstractmethod
    def partition_count(self) -> int:
        pass

    def exhausted(self) -> bool:
        return False


class KafkaStreamProducer(StreamProducer):
    def __init__(self, bootstrap_servers: str):
        # Idempotence keeps per-partition order and avoids duplicates across retries.
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )
        self._error: KafkaError | None = None

    def _on_delivery(self, err: KafkaError | None, _msg) -> None:
        if err is not None and self._error is None:
            self._error = err

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            err, self._error = self._error, None
            raise KafkaException(err)

    def async_send(self, topic: str, key: str, value: dict) -> None:
        self._raise_if_failed()
        payload = json.dumps(value).encode("utf-8")
        encoded_key = key.encode("utf-8") if key else None
        while True:
            try:
                self.producer.produce(
                    topic, key=encoded_key, value=payload, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self.producer.poll(0.1)
        self.producer.poll(0)

    def sync_send(self, topic: str, key: str, value: dict) -> None:
        self.async_send(topic, key, value)
        self.flush()

    def flush(self) -> None:
        self.producer.flush()
        self._raise_if_failed()


class KafkaStreamConsumer(StreamConsumer):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "latest",
        auto_commit: bool = True,
    ):
        self.topic = topic
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": auto_commit,
            }
        )
        self.consumer.subscribe([topic])

    def consume(self, timeout: int = 1000):
        return self.consume_stream(timeout)

    def consume_stream(self, timeout_ms: int = 1000):
        while True:  # Infinite loop for continuous streaming
            message = self.consumer.poll(timeout_ms / 1000)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            key = message.key().decode("utf-8") if message.key() else None
            value = (
                json.loads(message.value().decode("utf-8")) if message.value() else None
            )
            # Yields one message at a time, pausing the function here
            # until the caller asks for the next item.
            yield key, value

    def consume_batch(self, max_records: int, timeout_ms: int) -> list[StreamRecord]:
        records = []
        for message in self.consumer.consume(max_records, timeout_ms / 1000):
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            raw = message.value()
            key = message.key().decode("utf-8") if message.key() else None
            record = StreamRecord(
                message.topic(), message.partition(), message.offset(), key, None, raw
            )
            try:
                record.value = json.loads(raw) if raw else None
            except (ValueError, UnicodeDecodeError) as err:
                record.error = f"invalid JSON: {err}"
            records.append(record)
        return records

    def commit(self, records: list[StreamRecord]) -> None:
        next_offsets: dict[tuple[str, int], int] = {}
        for record in records:
            tp = (record.topic, record.partition)
            next_offsets[tp] = max(next_offsets.get(tp, 0), record.offset + 1)
        if next_offsets:
            self.consumer.commit(
                offsets=[TopicPartition(t, p, o) for (t, p), o in next_offsets.items()],
                asynchronous=False,
            )

    def partition_count(self) -> int:
        metadata = self.consumer.list_topics(self.topic, timeout=10)
        return len(metadata.topics[self.topic].partitions)

    def exhausted(self) -> bool:
        """True once every partition has been read up to the end of the log."""
        assigned = self.consumer.assignment()
        if len(assigned) < self.partition_count():
            return False
        for partition in assigned:
            position = self.consumer.position([partition])[0].offset
            # A consumer that has not fetched yet reports an invalid position;
            # the group's committed offset is how far it actually got.
            if position < 0:
                position = self.consumer.committed([partition], timeout=5)[0].offset
            _, end = self.consumer.get_watermark_offsets(partition, timeout=5)
            if position < end:
                return False
        return True

    def close(self) -> None:
        self.consumer.close()
