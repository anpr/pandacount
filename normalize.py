"""Normalisierung der Rohtexte: repariert ING-Whitespace-Zerhackung und
extrahiert Händler (PayPal/Amazon) für Matching und Session-Gruppierung."""

import re

import pandas as pd

_MERCHANT_RE = re.compile(r"ihreinkaufbei(.+)$")


def squash(text: str) -> str:
    """Lowercase + alle Whitespaces entfernt — macht zerhackte Strings matchbar."""
    return re.sub(r"\s+", "", text.lower())


def _as_str(value: object) -> str:
    return value if isinstance(value, str) else ""


def party_norm(party: object) -> str:
    text = re.sub(r"\s+", " ", _as_str(party).lower()).strip()
    return text.removeprefix("visa ").strip()


def extract_merchant(party: object, purpose: object) -> str:
    party_sq = squash(_as_str(party))
    if "paypal" in party_sq:
        match = _MERCHANT_RE.search(squash(_as_str(purpose)))
        if match and match.group(1):
            return match.group(1)
        return party_norm(party)
    if "amazon" in party_sq or "amzn" in party_sq:
        return "amazon"
    return party_norm(party)


def add_norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["party_norm"] = df["party"].map(party_norm)
    df["merchant"] = [extract_merchant(p, z) for p, z in zip(df["party"], df["purpose"])]
    return df
