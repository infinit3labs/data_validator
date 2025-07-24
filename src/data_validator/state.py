from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime


@dataclass
class PipelineState:
    """Persist validation progress to allow restarting pipelines."""

    path: Path
    state: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> "PipelineState":
        p = Path(path)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Migrate old format to new format
                if isinstance(data, dict) and data:
                    # Check if it's old format (string values) or new format (dict values)
                    first_value = next(iter(data.values()))
                    if isinstance(first_value, str):
                        # Convert old format to new format
                        migrated_data = {}
                        for table_name, status in data.items():
                            if status == "completed":
                                migrated_data[table_name] = {
                                    "status": "completed",
                                    "completed_at": datetime.now().isoformat(),
                                    "rules": {},
                                    "results": None
                                }
                        data = migrated_data
        else:
            data = {}
        return cls(path=p, state=data)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write atomically to prevent corruption
        temp_path = self.path.with_suffix('.tmp')
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2)
        temp_path.replace(self.path)

    def is_completed(self, table_name: str) -> bool:
        table_state = self.state.get(table_name, {})
        return table_state.get("status") == "completed"

    def is_rule_completed(self, table_name: str, rule_name: str) -> bool:
        """Check if a specific rule has been completed for a table."""
        table_state = self.state.get(table_name, {})
        rule_state = table_state.get("rules", {}).get(rule_name, {})
        return rule_state.get("status") == "completed"

    def mark_completed(self, table_name: str) -> None:
        if table_name not in self.state:
            self.state[table_name] = {
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "rules": {},
                "results": None
            }
        else:
            self.state[table_name]["status"] = "completed"
            self.state[table_name]["completed_at"] = datetime.now().isoformat()
        self.save()

    def mark_rule_completed(self, table_name: str, rule_name: str, result: Optional[Dict[str, Any]] = None) -> None:
        """Mark a specific rule as completed for a table."""
        if table_name not in self.state:
            self.state[table_name] = {
                "status": "in_progress",
                "started_at": datetime.now().isoformat(),
                "rules": {},
                "results": None
            }
        
        self.state[table_name]["rules"][rule_name] = {
            "status": "completed",
            "completed_at": datetime.now().isoformat(),
            "result": result
        }
        self.save()

    def mark_table_started(self, table_name: str) -> None:
        """Mark a table validation as started."""
        if table_name not in self.state:
            self.state[table_name] = {
                "status": "in_progress",
                "started_at": datetime.now().isoformat(),
                "rules": {},
                "results": None
            }
        else:
            self.state[table_name]["status"] = "in_progress"
            self.state[table_name]["started_at"] = datetime.now().isoformat()
        self.save()

    def store_results(self, table_name: str, results: Dict[str, Any]) -> None:
        """Store validation results for recovery purposes."""
        if table_name not in self.state:
            self.state[table_name] = {
                "status": "completed",
                "completed_at": datetime.now().isoformat(),
                "rules": {},
                "results": results
            }
        else:
            self.state[table_name]["results"] = results
            if self.state[table_name]["status"] != "completed":
                self.state[table_name]["status"] = "completed"
                self.state[table_name]["completed_at"] = datetime.now().isoformat()
        self.save()

    def get_cached_results(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached validation results for a table."""
        table_state = self.state.get(table_name, {})
        return table_state.get("results")

    def get_completed_rules(self, table_name: str) -> List[str]:
        """Get list of completed rule names for a table."""
        table_state = self.state.get(table_name, {})
        rules = table_state.get("rules", {})
        return [rule_name for rule_name, rule_state in rules.items() 
                if rule_state.get("status") == "completed"]

    def reset(self) -> None:
        self.state.clear()
        self.save()

    def reset_table(self, table_name: str) -> None:
        """Reset state for a specific table."""
        if table_name in self.state:
            del self.state[table_name]
            self.save()

    def reset_rule(self, table_name: str, rule_name: str) -> None:
        """Reset state for a specific rule within a table."""
        if table_name in self.state and "rules" in self.state[table_name]:
            rules = self.state[table_name]["rules"]
            if rule_name in rules:
                del rules[rule_name]
                self.save()
