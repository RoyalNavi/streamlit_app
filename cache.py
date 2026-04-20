import json
from pathlib import Path
from datetime import datetime

CACHE_DIR = Path(__file__).parent / "data" / "cache"


def write_cache(key: str, data) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": datetime.now().isoformat(), "data": data}
    (CACHE_DIR / f"{key}.json").write_text(
        json.dumps(payload, default=str, ensure_ascii=False)
    )


def read_cache(key: str) -> dict | None:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def cache_age_minutes(key: str) -> float | None:
    cached = read_cache(key)
    if not cached:
        return None
    try:
        updated = datetime.fromisoformat(cached["updated_at"])
        return (datetime.now() - updated).total_seconds() / 60
    except Exception:
        return None


def cache_freshness_label(key: str) -> str:
    age = cache_age_minutes(key)
    if age is None:
        return "jamais calcule"
    if age < 1:
        return "il y a moins d'1 min"
    if age < 60:
        return f"il y a {int(age)} min"
    return f"il y a {int(age / 60)}h{int(age % 60):02d}"
