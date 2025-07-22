"""Demonstration of running the CLI programmatically."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess


HERE = Path(__file__).parent
CONFIG = HERE / "sample_config.yaml"
SOURCES = HERE / "sources.yaml"
OUTPUT = HERE / "cli_report.json"


def main() -> None:
    cmd = [
        "data-validator",
        "--config",
        str(CONFIG),
        "--sources",
        str(SOURCES),
        "--output",
        str(OUTPUT),
    ]
    subprocess.check_call(cmd)
    print(json.loads(OUTPUT.read_text()))


if __name__ == "__main__":
    main()
