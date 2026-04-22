"""Configuration bootstrap helpers for the Streamlit app."""

from pathlib import Path
import os


def load_local_env_file(env_path: Path) -> None:
    """Load simple KEY=VALUE pairs from a local .env file when present."""
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for line in lines:
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#") or "=" not in cleaned:
            continue
        if cleaned.startswith("export "):
            cleaned = cleaned[len("export ") :].strip()
        key, value = cleaned.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = value
