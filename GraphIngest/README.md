# GraphIngest

Turns the payment stream into graphs and enriches accounts from the static user-profile file. `GraphIngestor.py` consumes Kafka and writes hopping windows in FalkorDB. A window covers 8 days, and a new window starts every 4 days. `Windows.py` holds that rule. `Accounts.py` loads the account registry (bank, account, entity) into each window before the payments. `WindowFlags.py` writes marks to `aml:flags` after the transfers in a window are stored. A mark is many counterparties, or one payment that left matching the payments that arrived.
