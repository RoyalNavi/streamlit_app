import json

import pandas as pd
import requests
import streamlit as st


CHANGE_DZ_RATES_URL = "https://changedz.fr/rates.json"


def normalize_changedz_rates_payload(payload: object) -> tuple[list[dict], str]:
    updated_at = ""
    if isinstance(payload, dict):
        for key in ("updated_at", "updatedAt", "last_update", "lastUpdate", "date", "timestamp"):
            if payload.get(key):
                updated_at = str(payload.get(key))
                break
        source = (
            payload.get("rates")
            or payload.get("data")
            or payload.get("currencies")
            or payload.get("items")
            or payload
        )
    else:
        source = payload

    rows: list[dict] = []
    if isinstance(source, dict):
        metadata_keys = {"updated_at", "updatedAt", "last_update", "lastUpdate", "date", "timestamp"}
        for key, value in source.items():
            if key in metadata_keys:
                continue
            if isinstance(value, dict):
                row = dict(value)
                row.setdefault("Devise", key)
            else:
                row = {"Devise": key, "Valeur": value}
            rows.append(row)
    elif isinstance(source, list):
        for index, value in enumerate(source, start=1):
            if isinstance(value, dict):
                rows.append(dict(value))
            else:
                rows.append({"Devise": f"Ligne {index}", "Valeur": value})

    return rows, updated_at


def normalize_changedz_rates_frame(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    flattened_rows: list[dict] = []
    for row in rows:
        flattened: dict = {}
        for key, value in row.items():
            normalized_key = str(key).strip().lower()
            if isinstance(value, dict):
                prefix = {"parallel": "Parallele", "official": "Officiel"}.get(normalized_key, str(key))
                for nested_key, nested_value in value.items():
                    flattened[f"{prefix} {nested_key}"] = nested_value
                if normalized_key == "parallel":
                    if "achat" in value:
                        flattened["Achat"] = value["achat"]
                    if "vente" in value:
                        flattened["Vente"] = value["vente"]
                    if "change" in value:
                        flattened["Variation"] = value["change"]
            else:
                flattened[key] = value
        flattened_rows.append(flattened)

    frame = pd.DataFrame(flattened_rows)
    rename_map = {}
    canonical_names = {
        "currency": "Devise",
        "curr": "Devise",
        "code": "Devise",
        "flag": "Drapeau",
        "symbol": "Devise",
        "name": "Nom",
        "label": "Nom",
        "buy": "Achat",
        "buy_rate": "Achat",
        "achat": "Achat",
        "sell": "Vente",
        "sell_rate": "Vente",
        "vente": "Vente",
        "rate": "Taux",
        "value": "Valeur",
        "valeur": "Valeur",
        "change": "Variation",
        "variation": "Variation",
        "updated_at": "Mis a jour",
        "date": "Date",
    }
    for column in frame.columns:
        normalized = str(column).strip().lower().replace(" ", "_").replace("-", "_")
        if normalized in canonical_names and canonical_names[normalized] not in frame.columns:
            rename_map[column] = canonical_names[normalized]

    frame = frame.rename(columns=rename_map)
    for column in frame.columns:
        frame[column] = frame[column].map(
            lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
        )

    preferred_order = [
        "Devise", "Drapeau", "Nom", "Achat", "Vente", "Variation",
        "Parallele achat", "Parallele vente", "Parallele change",
        "Officiel achat", "Officiel vente", "Taux", "Valeur", "Mis a jour", "Date",
    ]
    preferred = [column for column in preferred_order if column in frame.columns]
    hidden_raw_columns = {"parallel", "official"}
    others = [column for column in frame.columns if column not in preferred and str(column).strip().lower() not in hidden_raw_columns]
    return frame[preferred + others]


@st.cache_data(ttl=300, show_spinner=False)
def fetch_changedz_rates(user_agent: str) -> dict:
    response = requests.get(
        CHANGE_DZ_RATES_URL,
        headers={"User-Agent": user_agent, "Accept": "application/json"},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    rows, updated_at = normalize_changedz_rates_payload(payload)
    frame = normalize_changedz_rates_frame(rows)
    return {"updated_at": updated_at, "frame": frame, "payload": payload}

