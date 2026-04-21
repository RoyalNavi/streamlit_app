from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd


EUROPE_EQUITIES = [
    # France
    {"ticker": "MC.PA", "name": "LVMH", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "OR.PA", "name": "L'Oreal", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "TTE.PA", "name": "TotalEnergies", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "SAN.PA", "name": "Sanofi", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "AIR.PA", "name": "Airbus", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "RMS.PA", "name": "Hermes", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "BNP.PA", "name": "BNP Paribas", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "SU.PA", "name": "Schneider Electric", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "SAF.PA", "name": "Safran", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "DG.PA", "name": "VINCI", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "AI.PA", "name": "Air Liquide", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "CS.PA", "name": "AXA", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    # Germany
    {"ticker": "SAP.DE", "name": "SAP", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "SIE.DE", "name": "Siemens", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "ALV.DE", "name": "Allianz", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "DTE.DE", "name": "Deutsche Telekom", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "MUV2.DE", "name": "Munich Re", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "MBG.DE", "name": "Mercedes-Benz", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "BMW.DE", "name": "BMW", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "IFX.DE", "name": "Infineon", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "DB1.DE", "name": "Deutsche Boerse", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "BAS.DE", "name": "BASF", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    # Netherlands / Belgium
    {"ticker": "ASML.AS", "name": "ASML Holding", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "PRX.AS", "name": "Prosus", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "INGA.AS", "name": "ING Groep", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "ADYEN.AS", "name": "Adyen", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "HEIA.AS", "name": "Heineken", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "ABI.BR", "name": "Anheuser-Busch InBev", "exchange": "Euronext Brussels", "country": "Belgium", "currency": "EUR"},
    # Switzerland
    {"ticker": "NESN.SW", "name": "Nestle", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "NOVN.SW", "name": "Novartis", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "ROG.SW", "name": "Roche", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "ZURN.SW", "name": "Zurich Insurance", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "UBSG.SW", "name": "UBS Group", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "ABBN.SW", "name": "ABB", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    # Spain / Italy
    {"ticker": "ITX.MC", "name": "Inditex", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "IBE.MC", "name": "Iberdrola", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "SAN.MC", "name": "Banco Santander", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "BBVA.MC", "name": "BBVA", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "RACE.MI", "name": "Ferrari", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "ENEL.MI", "name": "Enel", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "UCG.MI", "name": "UniCredit", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "ISP.MI", "name": "Intesa Sanpaolo", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    # United Kingdom
    {"ticker": "SHEL.L", "name": "Shell", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "AZN.L", "name": "AstraZeneca", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "HSBA.L", "name": "HSBC Holdings", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "ULVR.L", "name": "Unilever", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "BP.L", "name": "BP", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "GSK.L", "name": "GSK", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "DGE.L", "name": "Diageo", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    # Nordics
    {"ticker": "NOVO-B.CO", "name": "Novo Nordisk", "exchange": "Nasdaq Copenhagen", "country": "Denmark", "currency": "DKK"},
    {"ticker": "ASML.AS", "name": "ASML Holding", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "ATCO-A.ST", "name": "Atlas Copco", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "INVE-B.ST", "name": "Investor", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "NOKIA.HE", "name": "Nokia", "exchange": "Nasdaq Helsinki", "country": "Finland", "currency": "EUR"},
    {"ticker": "EQNR.OL", "name": "Equinor", "exchange": "Oslo Bors", "country": "Norway", "currency": "NOK"},
    # Additional France
    {"ticker": "CAP.PA", "name": "Capgemini", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "ACA.PA", "name": "Credit Agricole", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "AL2SI.PA", "name": "2CRSI", "exchange": "Euronext Growth Paris", "country": "France", "currency": "EUR"},
    {"ticker": "EN.PA", "name": "Bouygues", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "RI.PA", "name": "Pernod Ricard", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "VIE.PA", "name": "Veolia", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "HO.PA", "name": "Thales", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "LR.PA", "name": "Legrand", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "ORA.PA", "name": "Orange", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "SGO.PA", "name": "Saint-Gobain", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    {"ticker": "STLAP.PA", "name": "Stellantis", "exchange": "Euronext Paris", "country": "France", "currency": "EUR"},
    # Additional Germany
    {"ticker": "ADS.DE", "name": "Adidas", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "BAYN.DE", "name": "Bayer", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "VOW3.DE", "name": "Volkswagen", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "RWE.DE", "name": "RWE", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "EOAN.DE", "name": "E.ON", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "MRK.DE", "name": "Merck KGaA", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "HEN3.DE", "name": "Henkel", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "SHL.DE", "name": "Siemens Healthineers", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "DHL.DE", "name": "DHL Group", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    {"ticker": "BEI.DE", "name": "Beiersdorf", "exchange": "XETRA", "country": "Germany", "currency": "EUR"},
    # Additional Netherlands / Belgium
    {"ticker": "PHIA.AS", "name": "Philips", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "KPN.AS", "name": "KPN", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "NN.AS", "name": "NN Group", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "AALB.AS", "name": "Aalberts", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "WKL.AS", "name": "Wolters Kluwer", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "UMG.AS", "name": "Universal Music Group", "exchange": "Euronext Amsterdam", "country": "Netherlands", "currency": "EUR"},
    {"ticker": "AGS.BR", "name": "Ageas", "exchange": "Euronext Brussels", "country": "Belgium", "currency": "EUR"},
    {"ticker": "KBC.BR", "name": "KBC Group", "exchange": "Euronext Brussels", "country": "Belgium", "currency": "EUR"},
    # Additional Switzerland
    {"ticker": "SIKA.SW", "name": "Sika", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "GIVN.SW", "name": "Givaudan", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "CFR.SW", "name": "Richemont", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "LONN.SW", "name": "Lonza", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "HOLN.SW", "name": "Holcim", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "SREN.SW", "name": "Swiss Re", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    {"ticker": "PGHN.SW", "name": "Partners Group", "exchange": "SIX Swiss Exchange", "country": "Switzerland", "currency": "CHF"},
    # Additional Spain / Italy
    {"ticker": "CABK.MC", "name": "CaixaBank", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "REP.MC", "name": "Repsol", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "FER.MC", "name": "Ferrovial", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "TEF.MC", "name": "Telefonica", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "AENA.MC", "name": "Aena", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "ACS.MC", "name": "ACS", "exchange": "BME", "country": "Spain", "currency": "EUR"},
    {"ticker": "ENI.MI", "name": "Eni", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "STLAM.MI", "name": "Stellantis", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "G.MI", "name": "Assicurazioni Generali", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "PRY.MI", "name": "Prysmian", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    {"ticker": "MONC.MI", "name": "Moncler", "exchange": "Borsa Italiana", "country": "Italy", "currency": "EUR"},
    # Additional United Kingdom
    {"ticker": "BATS.L", "name": "British American Tobacco", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "RIO.L", "name": "Rio Tinto", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "REL.L", "name": "RELX", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "LSEG.L", "name": "London Stock Exchange Group", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "PRU.L", "name": "Prudential", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "VOD.L", "name": "Vodafone", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "NG.L", "name": "National Grid", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "GLEN.L", "name": "Glencore", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "LLOY.L", "name": "Lloyds Banking Group", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "BARC.L", "name": "Barclays", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "RR.L", "name": "Rolls-Royce", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    {"ticker": "TSCO.L", "name": "Tesco", "exchange": "London Stock Exchange", "country": "United Kingdom", "currency": "GBp"},
    # Additional Nordics
    {"ticker": "VOLV-B.ST", "name": "Volvo", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "ASSA-B.ST", "name": "ASSA ABLOY", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "ERIC-B.ST", "name": "Ericsson", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "SEB-A.ST", "name": "SEB", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "SAND.ST", "name": "Sandvik", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "HM-B.ST", "name": "H&M", "exchange": "Nasdaq Stockholm", "country": "Sweden", "currency": "SEK"},
    {"ticker": "NDA-FI.HE", "name": "Nordea", "exchange": "Nasdaq Helsinki", "country": "Finland", "currency": "EUR"},
    {"ticker": "SAMPO.HE", "name": "Sampo", "exchange": "Nasdaq Helsinki", "country": "Finland", "currency": "EUR"},
    {"ticker": "KNEBV.HE", "name": "Kone", "exchange": "Nasdaq Helsinki", "country": "Finland", "currency": "EUR"},
    {"ticker": "DNB.OL", "name": "DNB Bank", "exchange": "Oslo Bors", "country": "Norway", "currency": "NOK"},
    {"ticker": "MOWI.OL", "name": "Mowi", "exchange": "Oslo Bors", "country": "Norway", "currency": "NOK"},
    {"ticker": "ORSTED.CO", "name": "Orsted", "exchange": "Nasdaq Copenhagen", "country": "Denmark", "currency": "DKK"},
    {"ticker": "MAERSK-B.CO", "name": "Maersk", "exchange": "Nasdaq Copenhagen", "country": "Denmark", "currency": "DKK"},
    {"ticker": "DSV.CO", "name": "DSV", "exchange": "Nasdaq Copenhagen", "country": "Denmark", "currency": "DKK"},
    {"ticker": "CARL-B.CO", "name": "Carlsberg", "exchange": "Nasdaq Copenhagen", "country": "Denmark", "currency": "DKK"},
]


RECOMMENDATION_EUROPE_TICKERS = [row["ticker"] for row in EUROPE_EQUITIES]


EUROPE_SUFFIXES = {
    ".PA", ".DE", ".AS", ".SW", ".MC", ".MI", ".L", ".BR", ".CO", ".ST", ".HE", ".OL",
}

SESSION_RULES = {
    "US": {"tz": "America/New_York", "regular": (time(9, 30), time(16, 0)), "pre": (time(4, 0), time(9, 30)), "post": (time(16, 0), time(20, 0))},
    "Europe": {"tz": "Europe/Paris", "regular": (time(9, 0), time(17, 30)), "pre": None, "post": None},
    "UK": {"tz": "Europe/London", "regular": (time(8, 0), time(16, 30)), "pre": None, "post": None},
}


def europe_equities_frame() -> pd.DataFrame:
    frame = pd.DataFrame(EUROPE_EQUITIES).drop_duplicates(subset=["ticker"], keep="first")
    frame["asset_type"] = "Entreprise"
    frame["region"] = "Europe"
    frame["market_region"] = "Europe"
    return frame


def recommendation_europe_equities() -> list[dict]:
    allowed = set(RECOMMENDATION_EUROPE_TICKERS)
    selected = []
    seen = set()
    for row in EUROPE_EQUITIES:
        ticker = row["ticker"]
        if ticker in allowed and ticker not in seen:
            selected.append(row)
            seen.add(ticker)
    return selected


def ticker_suffix(ticker: str) -> str:
    upper = str(ticker or "").upper()
    for suffix in sorted(EUROPE_SUFFIXES, key=len, reverse=True):
        if upper.endswith(suffix):
            return suffix
    return ""


def infer_market_region(ticker: str, asset_type: str | None = None) -> str:
    upper = str(ticker or "").upper()
    if upper.endswith("-USD") or asset_type == "Crypto":
        return "Crypto"
    if upper.startswith("^") or asset_type == "Indice":
        return "Global"
    if ticker_suffix(upper):
        return "Europe"
    return "US"


def infer_currency(ticker: str, default: str = "USD") -> str:
    suffix = ticker_suffix(ticker)
    if suffix in {".PA", ".DE", ".AS", ".MC", ".MI", ".BR", ".HE"}:
        return "EUR"
    if suffix == ".SW":
        return "CHF"
    if suffix == ".L":
        return "GBp"
    if suffix == ".CO":
        return "DKK"
    if suffix == ".ST":
        return "SEK"
    if suffix == ".OL":
        return "NOK"
    return default


def _local_now(region: str, now: datetime | None = None) -> datetime:
    current = now or datetime.utcnow()
    if current.tzinfo is None:
        current = current.replace(tzinfo=ZoneInfo("UTC"))
    rules = SESSION_RULES.get(region) or SESSION_RULES["US"]
    return current.astimezone(ZoneInfo(rules["tz"]))


def infer_market_session(ticker: str, now: datetime | None = None, asset_type: str | None = None) -> str:
    region = infer_market_region(ticker, asset_type)
    if region == "Crypto":
        return "24/7"
    if region == "Global":
        return "unknown"
    session_region = "UK" if str(ticker or "").upper().endswith(".L") else region
    rules = SESSION_RULES.get(session_region)
    if not rules:
        return "unknown"
    local = _local_now(session_region, now)
    if local.weekday() >= 5:
        return "closed"
    current_time = local.time()
    regular_start, regular_end = rules["regular"]
    if regular_start <= current_time < regular_end:
        return "regular"
    if rules.get("pre"):
        pre_start, pre_end = rules["pre"]
        if pre_start <= current_time < pre_end:
            return "pre_market"
    if rules.get("post"):
        post_start, post_end = rules["post"]
        if post_start <= current_time < post_end:
            return "after_hours"
    return "closed"


def latest_market_observation(hist: pd.DataFrame) -> tuple[str | None, float | None]:
    if hist is None or hist.empty or "Close" not in hist.columns:
        return None, None
    closes = hist["Close"].dropna()
    if closes.empty:
        return None, None
    timestamp = closes.index[-1]
    if hasattr(timestamp, "to_pydatetime"):
        timestamp = timestamp.to_pydatetime()
    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=ZoneInfo("UTC"))
        timestamp_text = timestamp.astimezone(ZoneInfo("UTC")).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    else:
        timestamp_text = str(timestamp)
    return timestamp_text, float(closes.iloc[-1])
