# Anti-Money Laundering

Illegal-funds are disguised as legal transactions amongst millions of incoming transactions and are often lost in the crowd. 
Investigating such frauds are very complicated due to the sheer size of transactions and lost paths being shrouded by other legitimate transactions. 

## How Verico helps

- Verico processes millions of transactions into Graphs, which are processed to be enriched by static user-profile information
- Verico allows investigating leads and cases which are generated during ingestion of transactions and transactions which are picked up by investigators
- Verico functions as a single source of information storing flagged suspicious accounts and corresponding payments
- Verico also automates the process of generating documentation throughout the case filing process
- Verico also helps in documenting the reasoning for the decision to keep or dismiss leads



## What Verico is not

- An agent which provides a final verdict on which accounts and transactions are fraud or illegal
- Investigators/Operators still have the final say on whether or not to classify illegal entities and payments



## Setup



### Libraries

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cd web && npm install
```



### Tokens

Copy `.env.example` to `.env`. The operator session and a lead review need one of `HF_TOKEN`, `GEMINI_API_KEY` (or `GOOGLE_API_KEY`), or `OPENAI_API_KEY`. `HUGGING_FACE_HUB_TOKEN` is read the same way as `HF_TOKEN`.

`INVESTIGATOR_PROVIDER` may be `gemini`, `openai`, or `huggingface`. Leave it empty and the first key that is set is used. `INVESTIGATOR_MODEL` overrides the default model for that provider.

Leave `DEMO_API_URL` empty on this machine. The chat API then listens on `127.0.0.1` port `8787`. Set `DEMO_API_URL` to a public URL only when the API should listen on `0.0.0.0`. `VERICO_ORIGIN` is an optional comma-separated list of browser origins. Local defaults are `http://localhost:3000` and `http://127.0.0.1:3000`.

The scripts expect Redis on `localhost` port `6379`, Kafka on `localhost` port `9092`, and FalkorDB on `localhost` port `6380`.

### Dataset

#### From transactions_files.zip

The zip is at the repo root. 

```bash
unzip transactions_files.zip
.venv/bin/python scripts/load_transactions.py --dir transactions_files
.venv/bin/python scripts/replay_transactions.py --duration 10m --topic transactions --partitions 6
.venv/bin/python scripts/ingest_graph.py --topic transactions --idle-exit 45 --accounts transactions_files/HI-Small_accounts.csv
.venv/bin/python scripts/detect_loops.py --topic transactions --idle-exit 30
.venv/bin/python scripts/flag_windows.py
.venv/bin/python scripts/explain_residues.py
```

The unzipped directory holds `HI-Small_Trans.csv`, `HI-Small_accounts.csv`, and `HI-Small_Patterns.txt`. Replay publishes the payment file onto the `transactions` topic. Ingest reads that topic from the start and writes the window graphs. `--idle-exit` stops ingest and loop detection after the topic goes quiet.

#### Custom dataset

Use the same columns. A payment file begins with this header. The loader appends the transaction id.

```text
Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering
```

Name the payment file so its stem ends in `_Trans`, for example `MySet_Trans.csv`. An account file uses `Bank Name,Bank ID,Account Number,Entity ID,Entity Name` and stays on disk. Ingest reads it with `--accounts`. Put both files in one directory.

```bash
.venv/bin/python scripts/load_transactions.py --dir /path/to/dataset
.venv/bin/python scripts/replay_transactions.py --dataset MySet_Trans --duration 10m
.venv/bin/python scripts/ingest_graph.py --accounts /path/to/dataset/accounts.csv --idle-exit 45
.venv/bin/python scripts/detect_loops.py --topic transactions --idle-exit 30
.venv/bin/python scripts/flag_windows.py
.venv/bin/python scripts/explain_residues.py
```



### Workspace

The site reads the graphs already in FalkorDB. The slice under `presentation/slice` loads a smaller graph without Kafka.

```bash
.venv/bin/python scripts/load_presentation_slice.py --host 127.0.0.1
.venv/bin/python scripts/detect_slice_loops.py --payments presentation/slice/payments.csv
.venv/bin/python scripts/explain_residues.py --one-each
.venv/bin/python scripts/demo_api.py
```

In another terminal:

```bash
cd web && npm run dev
```

The site is at `http://localhost:3000`. The workspace is `/investigate` and talks to the chat API on port `8787`. From the terminal, an investigator can ask:

```bash
.venv/bin/python scripts/operator_pipeline.py --question "List the leads I can open."
```



## Structure of Repo

- `adapters` holds the Redis, Kafka, and FalkorDB connections.
- `Detection` finds loops and residue cases in the payments.
- `GraphIngest` writes the stream into window graphs and stores marks on accounts.
- `GraphRead` reads those graphs for an investigator. The MCP server is here.
- `Investigation` reviews one lead and stores keep or dismiss.
- `Operator` runs the session that answers the investigator and writes the case book after a keep.
- `StreamReplay` loads a payment file and replays it onto Kafka.
- `scripts` are the commands for those steps, including the workspace chat.
- `presentation` holds the small payment slice, saved tool replies, pipeline output, and terminal logs.
- `web` is the site: the front page, the method page, and the workspace.
- `images` holds the method pictures. The site serves the copies under `web/public/method`.
- `transactions_files` is the unzipped payment directory. It is gitignored. `transactions_files.zip` stays at the repo root.

`requirements.txt` and `.env.example` sit at the repo root.

## Scripts

- `scripts/load_transactions.py` loads the payment files into Redis. A file whose stem ends in `_Trans` is stored as a dataset. Other files in the directory are stored as raw bytes.
- `scripts/replay_transactions.py` replays a stored dataset onto Kafka, compressing its original time span into a shorter wall-clock duration.
- `scripts/ingest_graph.py` consumes that topic into rolling FalkorDB window graphs and loads the account registry into each window.
- `scripts/detect_loops.py` reads the same topic and writes loops to `aml:loops`.
- `scripts/write_open_chains.py` replays the stored payments and writes unfinished chains into `aml:loops`. Closed loops already stored are left as they are.
- `scripts/evaluate_loops.py` runs the loop detector offline over a stored dataset and scores the cycles against the file's labels and pattern list. Those labels are used only in this script.
- `scripts/flag_windows.py` runs the window checks over transfers already in FalkorDB and writes marks to `aml:flags`.
- `scripts/explain_residues.py` writes pooled and split residue cases for transfers already in FalkorDB. `--one-each` stops after one pooled case and one split case.
- `scripts/build_presentation_slice.py` cuts `presentation/slice` from the payment files and leads already stored in the full graph. It does not write to FalkorDB or Redis.
- `scripts/load_presentation_slice.py` loads `presentation/slice` into FalkorDB window graphs. No Kafka.
- `scripts/detect_slice_loops.py` detects closed loops in the slice and writes them to FalkorDB. Unfinished chains are not written.
- `scripts/investigate_lead.py` reviews one stored lead and writes keep or dismiss to `aml:decisions`. The argument is a loop key or a residue id.
- `scripts/operator_pipeline.py` checks the MCP server, then answers one question. Replies go to `presentation/operator-pipeline`.
- `scripts/simulate_conversation.py` runs one operator conversation with a stand-in model. The MCP tools are the real ones. No API key.
- `scripts/mcp_example.py` calls the `aml-graph` MCP tools and writes each reply to `presentation/mcp-example`.
- `scripts/demo_api.py` serves the workspace chat on port `8787`.
- `scripts/cli_utils.py` parses durations such as `90s`, `10m`, `1h30m`, and `8d` for the other scripts.



## Leads and Cases

- A lead is what an investigator opens. `list_leads` takes `loop`, `residue`, `review`, or `all`.
- A loop is money that left an account and later came back. Loops are stored in `aml:loops`. The key is the payment ids joined by commas. An open chain is two or more similar hops that never pay back into an account already on the path. `scripts/write_open_chains.py` writes those. Closed loops stay closed.
- A residue case is what remains after a similar incoming payment and a later outgoing payment are paired off. Cases are stored in `aml:cases`. `pooled` means several incoming payments together match one outgoing payment. `split` means one incoming payment matches several outgoing payments. The workspace reply describes that in ordinary words. The kind names stay on the case.
- A counterparty review is written when an investigator asks for the accounts that dealt with one already open. One lead is stored per counterparty, in `aml:reviews`, with kind `counterparty`. `list_leads` returns them only when `kind` is `review`. Asking for all leads leaves them out.
- A mark sits on an account in `aml:flags`. `fan_in` is many accounts sending money to this one. `fan_out` is this one sending money to many accounts. `gather` is one payment that left matching the payments that arrived. `flags_for_account` reads the mark.
- Keep or dismiss is stored in `aml:decisions`. Keep means a person should look at the payments. Dismiss means the payments read as ordinary. The case book is the workbook under `presentation/cases`. It is written when the investigator keeps the lead and asks for it. A dismiss does not open one.

## Stored graphs

FalkorDB holds the graphs. Redis, on port `6379`, holds the payment files and the conversation turns. The window list and the ingest watermark are hashes on the FalkorDB port.

| Name | Store | What it holds |
| --- | --- | --- |
| `aml:txn:YYYYMMDDTHHMM` | FalkorDB | One window of payments, plus the accounts in that window. The stamp is the window start. |
| `aml:txn:windows` | FalkorDB | Hash of each window name, its start and end, and whether it is still open. |
| `aml:txn:watermark` | FalkorDB | How far ingest has written, one field per Kafka partition. |
| `aml:loops` | FalkorDB | Closed loops. `scripts/write_open_chains.py` also writes unfinished chains here. |
| `aml:cases` | FalkorDB | Pooled and split residue cases. |
| `aml:flags` | FalkorDB | Marks on an account: fan-in, fan-out, and gather. |
| `aml:reviews` | FalkorDB | One counterparty lead for each account pair an investigator asked to open. |
| `aml:decisions` | FalkorDB | Keep or dismiss for a lead, with the reason and the payment ids. |
| `aml:sessions` | FalkorDB | One node per conversation, linked to the leads, accounts, and payments it touched. |
| `aml:tmp:{id}` | FalkorDB | A graph built while explaining a set of payments. The call deletes it before returning. |
| `aml:log:{session}` | Redis | The turns of one conversation. |
| `{dataset}` | Redis | A payment file whose name ends in `_Trans`, plus its hourly buckets `{dataset}:{hour}`. Other files from the load directory are stored under their file stem. |

## Graph data model

An account id is the bank number with leading zeros removed, a colon, and the account number. The same id is how a payment in one window is found in the next. A window graph has no edge into another window.

A payment in a window graph, and in the short-lived explain graph, is:

```cypher
(:Account)-[:TRANSFER {
  txn_id, ts, amount_paid, payment_currency,
  amount_received, receiving_currency, payment_format
}]->(:Account)
```

`Account` also stores `bank`, `bank_name`, `entity_id`, and `entity_name`. Accounts that share an `entity_id` are the same party and stay separate nodes.

A loop in `aml:loops` is:

```cypher
(:Account)-[:IN_LOOP {position}]->(:Loop)
```

`Loop` stores `key`, `txn_ids`, `length`, `start_ts`, `end_ts`, `amount_out`, `amount_back`, `tree_root`, `closed_at_root`, and `open`. The key is the payment ids joined by commas. `position` is the account's place on that path.

The other records are nodes with no edge between them. The account they name is a property.

| Label | Graph | Properties |
| --- | --- | --- |
| `Case` | `aml:cases` | `id`, `kind` (`pooled` or `split`), `account`, `window`, `focal_txn`, `part_txns`, `amount`, `part_sum`, `currency`, `ts` |
| `Flag` | `aml:flags` | `id`, `rule` (`fan_in`, `fan_out`, or `gather`), `account`, `window`, `parties`, `txn_id`, `amount`, `inflow_sum`, `inflow_txns`, `ts` |
| `Review` | `aml:reviews` | `id`, `kind` (`counterparty`), `account`, `counterparty`, `txn_ids`, `window`, `ts` |
| `Decision` | `aml:decisions` | `lead`, `decision`, `kind`, `reason`, `accounts`, `txn_ids`, `recorded_ts` |
| `Session` | `aml:sessions` | `id`, `updated_ts`, and `sheet` once a case book is written |
| `Subject` | `aml:sessions` | `key`, such as `loop:393953,477907` or `account:21:80B196ED0` |

A session points at what the conversation touched: `(:Session)-[:ABOUT]->(:Subject)`.

## Queries and algorithms

Ingest writes each window with two queries in `GraphIngest/GraphIngestor.py`. `WRITE_ACCOUNTS` merges `Account` by id. `WRITE_TRANSFERS` merges `TRANSFER` by `txn_id`, so a retried payment does not become a second edge. Indexes are on `Account.id`, `Account.entity_id`, `TRANSFER.txn_id`, and `TRANSFER.ts`.

`Detection/TreeLoopDetector.py` walks the payments in time order in Python. A later outgoing payment continues a branch when its amount stays near the amount already followed. A payment back to an account already on that branch is a closed loop. Two or more hops that never return are an open chain. One payment on its own is not. `Detection/LoopSink.py` writes both with `WRITE_LOOPS`.

`Detection/ResidueCases.py` reads a window with two queries, then finishes the match in Python. `ELIGIBLE` keeps accounts whose distinct senders and receivers are inside the spread limit. `EDGES` returns the payments on those accounts. Similar one-to-one pairs are set aside. What remains is a pooled case or a split case. `WRITE_CASES` stores it.

`GraphIngest/WindowFlags.py` reads the same windows. `FAN_IN` and `FAN_OUT` count distinct counterparties. `GATHER_CANDIDATES` finds accounts with enough incoming payments to test, and the sum check runs in Python. `WRITE_FLAGS` stores the mark.

The reads the workspace uses are in `GraphRead/reader.py`, `GraphRead/explain.py`, and `GraphRead/reviews.py`.

| Read | Query | What it returns |
| --- | --- | --- |
| Payments on an account | `MATCH (a:Account {id})-[t:TRANSFER]->(b)` and the reverse | Incoming and outgoing payments. A self-payment is left out unless the call asks for it. |
| A loop | `MATCH (a)-[r:IN_LOOP]->(l:Loop)` | The loop for an account, or the accounts on a loop key. |
| The same party | `MATCH (a:Account {entity_id})` | Other accounts of that entity. |
| One cited payment | `MATCH (a)-[t:TRANSFER]->(b) WHERE t.txn_id = $id` | The edge, looked up by payment id with the ends unlabeled so Falkor uses the `txn_id` index. |
| How cited payments meet | copy those edges into `aml:tmp:{id}`, then `MATCH (a)-[t:TRANSFER]->(b)` | Payments that share an account come back as one graph. The temporary graph is deleted before the call returns. |
| A counterparty lead | `MERGE (r:Review {id})` | One review for the flagged account and one counterparty, written when the investigator asks. |

