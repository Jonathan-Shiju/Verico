import argparse
import re

DURATION_UNITS = {"d": 86400, "h": 3600, "m": 60, "s": 1}


def parse_duration(value: str) -> float:
    text = value.strip().lower()
    parts = re.findall(r"(\d+(?:\.\d+)?)([dhms])", text)
    if not parts or "".join(n + u for n, u in parts) != text:
        raise argparse.ArgumentTypeError(
            f"Invalid duration '{value}', use e.g. 90s, 10m, 1h30m, 8d"
        )
    return sum(float(n) * DURATION_UNITS[u] for n, u in parts)
