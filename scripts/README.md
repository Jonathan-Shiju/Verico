# scripts

Commands that load payments, build the graphs, find leads, and open the workspace. `load_transactions.py` and `replay_transactions.py` fill Redis and Kafka. `ingest_graph.py`, `detect_loops.py`, `flag_windows.py`, and `explain_residues.py` write the graphs an investigator reads. `load_presentation_slice.py`, `detect_slice_loops.py`, and `demo_api.py` are the smaller path for the site. `operator_pipeline.py` answers one question through the MCP server. `investigate_lead.py` writes keep or dismiss for one stored lead.
