# StreamReplay

Loads a payment file and replays it onto Kafka. `TransactionReplay.py` stores an IBM AML transactions CSV in Redis as hourly buckets, so the file can be read back in time order without holding the whole file in memory. The replayer compresses the original time span into a shorter wall-clock duration and keeps the relative spacing. `StreamTransaction.py` reads a stored file back from Redis and sends it on the stream, or listens on a consumer.
