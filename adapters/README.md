# adapters

The connections the rest of the repo calls. `staticdb.py` is Redis, used for the payment file and for the conversation log. `streamdb.py` is Kafka, the payment stream the ingestor and the loop detector consume. `graphdb.py` is FalkorDB, where the window graphs, loops, cases, marks, and decisions are stored. Defaults are Redis on port `6379`, Kafka on port `9092`, and FalkorDB on port `6380`.
