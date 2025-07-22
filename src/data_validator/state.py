from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
from typing import Dict


@dataclass
class PipelineState:
    """Persist validation progress to allow restarting pipelines."""

    path: Path
    state: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> "PipelineState":
        p = Path(path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}
        return cls(path=p, state=data)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)

    def is_completed(self, table_name: str) -> bool:
        return self.state.get(table_name) == "completed"

    def mark_completed(self, table_name: str) -> None:
        self.state[table_name] = "completed"
        self.save()

    def reset(self) -> None:
        self.state.clear()
        self.save()
