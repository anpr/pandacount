"""Kategorisierungsregeln: Laden, Validieren und Anwenden von rules.yml.

Match-Semantik (identisch zum historischen Dict in panda.py):
case-insensitives Substring-Matching pro Attribut; spätere Regeln überschreiben
frühere; account = Gleichheit; amount_min/amount_max sind exklusive Schranken.
"""

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import yaml

RULES_PATH = Path(__file__).parent / "rules.yml"

MATCH_ATTRIBUTES = ("party", "purpose", "book_text")
RULE_FIELDS = {"category", "match", "account", "amount_min", "amount_max"}


@dataclass(frozen=True)
class Rule:
    category: str
    match: dict[str, tuple[str, ...]] = field(default_factory=dict)
    account: str | None = None
    amount_min: float | None = None
    amount_max: float | None = None


def _parse_rule(index: int, raw: dict) -> Rule:
    def err(msg: str) -> ValueError:
        return ValueError(f"rules.yml Regel #{index} ({raw.get('category', '?')}): {msg}")

    unknown = set(raw) - RULE_FIELDS
    if unknown:
        raise err(f"unbekannte Felder {sorted(unknown)}")
    if not isinstance(raw.get("category"), str) or not raw["category"]:
        raise err("category fehlt oder ist kein String")
    match_raw = raw.get("match")
    if not isinstance(match_raw, dict) or not match_raw:
        raise err("match fehlt oder ist leer")
    match: dict[str, tuple[str, ...]] = {}
    for attr, patterns in match_raw.items():
        if attr not in MATCH_ATTRIBUTES:
            raise err(f"unbekanntes Match-Attribut {attr!r} (erlaubt: {MATCH_ATTRIBUTES})")
        if not isinstance(patterns, list) or not patterns:
            raise err(f"match.{attr} muss eine nicht-leere Liste sein")
        if not all(isinstance(p, str) and p for p in patterns):
            raise err(f"match.{attr}: alle Patterns müssen nicht-leere Strings sein")
        match[attr] = tuple(patterns)
    return Rule(
        category=raw["category"],
        match=match,
        account=raw.get("account"),
        amount_min=raw.get("amount_min"),
        amount_max=raw.get("amount_max"),
    )


def load_rules(path: Path = RULES_PATH) -> list[Rule]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("rules.yml muss eine Liste von Regeln sein")
    return [_parse_rule(i, entry) for i, entry in enumerate(raw)]


def _rule_mask(df: pd.DataFrame, rule: Rule) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    for attr, patterns in rule.match.items():
        values = df[attr].fillna("").str.lower()
        for pattern in patterns:
            mask |= values.str.contains(pattern.lower(), regex=False)
    if rule.account is not None:
        mask &= df["account"] == rule.account
    if rule.amount_min is not None:
        mask &= df["amount"] > rule.amount_min
    if rule.amount_max is not None:
        mask &= df["amount"] < rule.amount_max
    return mask


def apply_rules(df: pd.DataFrame, rules: list[Rule]) -> pd.DataFrame:
    if "category" not in df.columns:
        df["category"] = pd.NA
    for rule in rules:
        df.loc[_rule_mask(df, rule), "category"] = rule.category
    return df
