# Terminal logs

Copies of Cursor terminal sessions for the pipeline. Each file includes the session metadata header (`pid`, `cwd`, `command`, `started_at`, `status`) and the command output. Log line timestamps are local time (UTC+5:30). Header timestamps are UTC.

No Redis-load session was in the terminal folder. The completed ingest log records the account registry used for the windows: 518,581 accounts.

Watermarks are the progress lines inside the ingest logs.

## Completed pipeline runs

### `kafka-replay.log`

Command: `.venv/bin/python scripts/replay_transactions.py --duration 10m --topic transactions --partitions 6`

Started 2026-10-15T02:19:05Z, ended 2026-10-15T02:28:33Z, exit 0.

The replay sent 5,078,345 rows (2022-09-01 00:00:00 through 2022-09-18 16:18:00) in 600.0s, with a reported max lag of 1.12s.

### `ingest-graph.log`

Command: `.venv/bin/python scripts/ingest_graph.py --topic transactions --group aml-graph-ingest-v2 --idle-exit 45`

Started 2026-10-15T03:33:33Z, ended 2026-10-15T04:03:49Z, exit 0.

The completed FalkorDB window ingest registered 518,581 accounts, wrote 5,078,345 transactions with `dead_lettered` 0, and the last watermark is `2022/09/16 06:00`. Windows logged as closed are `aml:txn:20220828T0000`, `aml:txn:20220901T0000`, and `aml:txn:20220905T0000`.

### `detect-loops-rebuild.log`

Command: `docker exec falkordb redis-cli GRAPH.DELETE aml:loops; .venv/bin/python scripts/detect_loops.py --topic transactions --idle-exit 30`

Started 2026-10-15T14:39:49Z, ended 2026-10-15T14:53:11Z, exit 0. Session title: "Rebuild loop graph with unpadded account ids".

The completion line is `Done in 767s` with `transactions` 5,078,345 and `cycles` 4,863 (`hubs` 15).

### `flag-windows.log`

Command: `.venv/bin/python scripts/flag_windows.py`

Started 2026-10-15T14:43:08Z, ended 2026-10-15T15:49:31Z, exit 0. The file is the finished session (elapsed 4,219,403 ms).

Per-window done lines:

- `aml:txn:20220828T0000 done: accounts=511,548 flags=4,675 in 1388s`
- `aml:txn:20220901T0000 done: accounts=514,022 flags=8,638 in 1177s`
- `aml:txn:20220905T0000 done: accounts=378,268 flags=3,830 in 842s`
- `aml:txn:20220909T0000 done: accounts=354,584 flags=1,674 in 809s`
- `aml:txn:20220913T0000 done: accounts=292 flags=9 in 1s`
- `aml:txn:20220917T0000 done: accounts=29 flags=0 in 0s`

## Earlier ingest sessions

These exited before a full write of 5,078,345 rows. The completed run is `ingest-graph.log`.

### `ingest-graph-alongside-replay.log`

Command: `sleep 5; .venv/bin/python scripts/ingest_graph.py --topic transactions --idle-exit 60`

Started 2026-10-15T02:19:06Z, ended 2026-10-15T02:47:38Z, exit 143.

The last progress line shows `written=2,207,070` and watermark `2022/09/04 01:20`.

### `ingest-graph-early.log`

Command: `.venv/bin/python scripts/ingest_graph.py --topic transactions --idle-exit 60`

Started 2026-10-15T02:16:17Z, ended 2026-10-15T02:17:52Z, exit code unknown.

The last progress line shows `written=517,476` and watermark `2022/09/01 06:10`.

### `ingest-graph-restart.log`

Command: `.venv/bin/python scripts/ingest_graph.py --topic transactions --idle-exit 90`

Started 2026-10-15T02:00:24Z, ended 2026-10-15T02:01:03Z, exit code unknown.

The last progress line shows `written=63,469` and watermark `-`.

### `ingest-graph-startup-error.log`

Command: background `scripts/replay_transactions.py` (output redirected to `/tmp/replay.log`, not in this file), then `.venv/bin/python scripts/ingest_graph.py --topic transactions --idle-exit 90`

Started 2026-10-15T02:00:00Z, ended 2026-10-15T02:00:16Z, exit 1.

The ingestor raised `AttributeError: 'str' object has no attribute 'decode'` before writing a progress line.

## MCP

### `mcp-list.log`

Command: `gemini --skip-trust mcp list`

Started 2026-10-15T04:27:12Z, ended 2026-10-15T04:28:38Z, status aborted.

The process aborted during Gemini authentication and did not print a server list. The log contains no credential values.
