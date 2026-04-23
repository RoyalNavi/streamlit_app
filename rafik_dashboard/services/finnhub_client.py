import logging
import os
from typing import Any

import requests


FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
FINNHUB_TIMEOUT_SECONDS = 12

logger = logging.getLogger(__name__)
_SESSION = requests.Session()
_missing_key_warned = False


def get_finnhub_api_key() -> str:
    return os.getenv("FINNHUB_API_KEY", "").strip()


def is_finnhub_configured() -> bool:
    return bool(get_finnhub_api_key())


def get(endpoint: str, params: dict[str, Any] | None = None, *, timeout: int = FINNHUB_TIMEOUT_SECONDS) -> Any | None:
    """Fetch a Finnhub JSON endpoint with shared auth, timeout and error handling."""
    global _missing_key_warned

    token = get_finnhub_api_key()
    if not token:
        if not _missing_key_warned:
            logger.warning("FINNHUB_API_KEY absente: enrichissement Finnhub desactive.")
            _missing_key_warned = True
        return None

    cleaned_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"
    request_params = dict(params or {})
    request_params["token"] = token

    try:
        response = _SESSION.get(
            f"{FINNHUB_BASE_URL}{cleaned_endpoint}",
            params=request_params,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except requests.Timeout:
        logger.warning("Timeout Finnhub sur %s.", cleaned_endpoint)
    except requests.RequestException as exc:
        logger.warning("Erreur Finnhub sur %s: %s", cleaned_endpoint, exc)
    except ValueError as exc:
        logger.warning("Reponse JSON Finnhub invalide sur %s: %s", cleaned_endpoint, exc)
    return None
