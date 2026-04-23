#!/usr/bin/env python3
from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

RECOMMENDATION_FILES = [
    BASE_DIR / "data" / "cache" / "stock_ideas.json",
    BASE_DIR / "data" / "cache" / "stock_ideas_meta.json",
    BASE_DIR / "data" / "cache" / "daily_universe.json",
    BASE_DIR / "data" / "signals.sqlite3",
]


def archive_recommendation_state() -> Path:
    archive_dir = BASE_DIR / "data" / "archive" / f"recommendations_reset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    archive_dir.mkdir(parents=True, exist_ok=False)

    moved = []
    for path in RECOMMENDATION_FILES:
        if not path.exists():
            continue
        destination = archive_dir / path.relative_to(BASE_DIR)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(destination))
        moved.append(path.relative_to(BASE_DIR))

    manifest = archive_dir / "MANIFEST.txt"
    manifest.write_text(
        "Recommendation reset archive\n"
        f"Created at: {datetime.now().isoformat(timespec='seconds')}\n\n"
        "Moved files:\n"
        + ("\n".join(f"- {item}" for item in moved) if moved else "- none")
        + "\n\nConserved files include users.sqlite3, watchlist caches, market directories and news caches.\n",
        encoding="utf-8",
    )
    return archive_dir


def main() -> int:
    archive_dir = archive_recommendation_state()

    import worker

    worker.init_signal_db()
    print(f"Archived recommendation state to: {archive_dir}")
    print("Recreated empty signal database at data/signals.sqlite3")
    print("Next step: restart the worker or run `.venv/bin/python -c \"import worker; worker.job_score_stocks()\"`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
