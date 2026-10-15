import csv
from pathlib import Path


def account_id(bank: str, number: str) -> str:
    """Bank ids arrive zero-padded in transactions and unpadded in the account
    registry; the numeric form is the same bank."""
    return f"{int(bank)}:{number}"


def load_account_registry(path: Path) -> dict[str, dict]:
    """Accounts registered before any transaction. Keyed by account_id."""
    registry = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            ident = account_id(row["Bank ID"], row["Account Number"])
            registry[ident] = {
                "id": ident,
                "bank": str(int(row["Bank ID"])),
                "bank_name": row["Bank Name"],
                "entity_id": row["Entity ID"],
                "entity_name": row["Entity Name"],
            }
    return registry
