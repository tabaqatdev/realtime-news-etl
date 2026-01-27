"""
State Management Module
Tracks processed files to enable incremental processing
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """Manages pipeline state for incremental processing"""

    def __init__(self, state_file: Path | None = None):
        self.state_file = state_file or Path("data/.pipeline_state.json")
        self.state = self._load_state()

    def _load_state(self) -> dict:
        """Load state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}, starting fresh")
                return self._empty_state()
        return self._empty_state()

    def _empty_state(self) -> dict:
        """Create empty state structure"""
        return {
            "processed_files": {},  # filename -> {date, status, timestamp}
            "last_run": None,
            "daily_consolidated": {},  # date -> True/False
        }

    def _save_state(self):
        """Save state to file"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def is_file_processed(self, filename: str) -> bool:
        """Check if a file has already been processed"""
        return filename in self.state["processed_files"]

    def mark_file_processed(self, filename: str, status: str = "success"):
        """Mark a file as processed"""
        self.state["processed_files"][filename] = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "date": filename[:8] if len(filename) >= 8 else None,
        }
        self._save_state()

    def is_day_consolidated(self, date_str: str) -> bool:
        """Check if a day has been consolidated"""
        return self.state["daily_consolidated"].get(date_str, False)

    def mark_day_consolidated(self, date_str: str):
        """Mark a day as consolidated"""
        self.state["daily_consolidated"][date_str] = True
        self.state["last_run"] = datetime.now().isoformat()
        self._save_state()

    def get_processed_files_for_date(self, date_str: str) -> list[str]:
        """Get all processed files for a specific date"""
        return [
            filename
            for filename, info in self.state["processed_files"].items()
            if info.get("date") == date_str and info.get("status") == "success"
        ]

    def get_stats(self) -> dict:
        """Get state statistics"""
        total_files = len(self.state["processed_files"])
        successful = sum(
            1 for f in self.state["processed_files"].values() if f.get("status") == "success"
        )
        consolidated_days = len(
            [d for d, status in self.state["daily_consolidated"].items() if status]
        )

        return {
            "total_files_processed": total_files,
            "successful": successful,
            "failed": total_files - successful,
            "consolidated_days": consolidated_days,
            "last_run": self.state.get("last_run"),
        }

    def reset(self):
        """Reset state (use with caution)"""
        self.state = self._empty_state()
        self._save_state()
        logger.info("State reset complete")
