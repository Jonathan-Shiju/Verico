# GraphRead

Reads the graphs for an investigator. Window graphs, loops, residue cases, marks, and keep or dismiss records are queried here. `server.py` is the MCP server, named `aml-graph`, that the operator session starts. Window graphs, loops, cases, and flags stay read-only through that server. A keep or dismiss is written only to `aml:decisions`. A counterparty lead the investigator orders is written only to `aml:reviews`.

`reader.py` is the shared read over FalkorDB. `operator_reads.py` lists leads, summarizes an account or an entity, and walks later payments from one payment. `explain.py` builds a short graph of the payments a reason cites. `reviews.py` opens one lead for each account that dealt with the one named.
