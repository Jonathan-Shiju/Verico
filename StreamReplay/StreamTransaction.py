from adapters.staticdb import StaticDB
from adapters.streamdb import StreamProducer, StreamConsumer
import json


class ProduceStream:
    def __init__(self, stream_producer: StreamProducer, static_db: StaticDB):
        self.stream_producer = stream_producer
        self.static_db = static_db

    def get_file(self, file_id: str) -> json:
        # Retrieve the file from the static database
        data, metadata = self.static_db.get(file_id)
        if data is None:
            raise FileNotFoundError(f"File with ID {file_id} not found.")
        return {"data": data.decode(), "metadata": metadata}

    def asycn_send(self, topic: str, file_id: str) -> None:
        # Retrieve the file from the static database
        file_data = self.get_file(file_id)
        # Send the file data to the stream
        self.stream_producer.async_send(topic, key=file_id, value=file_data)
        self.stream_producer.flush()

    def sycn_send(self, topic: str, file_id: str) -> None:
        # Retrieve the file from the static database
        file_data = self.get_file(file_id)
        # Send the file data to the stream
        self.stream_producer.sync_send(topic, key=file_id, value=file_data)


class ConsumeStream:
    def __init__(self, stream_consumer: StreamConsumer):
        self.stream_consumer = stream_consumer

    def listen(self, timeout: int = 1000):
        for key, value in self.stream_consumer.consume_stream(timeout):
            yield {"key": key, "value": value}
