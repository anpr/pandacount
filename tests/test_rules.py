from pathlib import Path

import pandas as pd
import pytest

from rules import Rule, apply_rules, load_rules


def _df(**overrides) -> pd.DataFrame:
    base = {
        "account": ["giro", "gesa", "common"],
        "party": ["BIO COMPANY GmbH", "VISA APPLE.COM/BILL", None],
        "purpose": [None, "Abo", "Spotify AB 123"],
        "book_text": ["Lastschrift", "Lastschrift", "Lastschrift"],
        "amount": [-20.0, -9.99, -12.0],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def test_substring_match_case_insensitive() -> None:
    rules = [Rule(category="einkaufen", match={"party": ("bio company",)})]
    out = apply_rules(_df(), rules)
    assert out.loc[0, "category"] == "einkaufen"
    assert pd.isna(out.loc[1, "category"])


def test_later_rule_wins() -> None:
    rules = [
        Rule(category="einkaufen", match={"purpose": ("spotify",)}),
        Rule(category="freizeit", match={"purpose": ("spotify ab",)}),
    ]
    out = apply_rules(_df(), rules)
    assert out.loc[2, "category"] == "freizeit"


def test_account_constraint() -> None:
    rules = [Rule(category="persoenlich", match={"party": ("apple",)}, account="common")]
    out = apply_rules(_df(), rules)
    assert pd.isna(out.loc[1, "category"])  # apple ist auf gesa, nicht common


def test_amount_min_exclusive() -> None:
    rules = [Rule(category="freizeit::media", match={"party": ("apple",)}, amount_min=-50.0)]
    out = apply_rules(_df(), rules)
    assert out.loc[1, "category"] == "freizeit::media"  # -9.99 > -50
    out2 = apply_rules(_df(amount=[-20.0, -60.0, -12.0]), rules)
    assert pd.isna(out2.loc[1, "category"])  # -60 < -50


def test_load_rules_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "rules.yml"
    p.write_text(
        '- category: einkaufen\n  match: {party: ["bio company"]}\n'
        '- category: freizeit::media\n  match: {party: ["apple"]}\n'
        "  account: gesa\n  amount_min: -50\n",
        encoding="utf-8",
    )
    rules = load_rules(p)
    assert rules[0] == Rule(category="einkaufen", match={"party": ("bio company",)})
    assert rules[1].account == "gesa" and rules[1].amount_min == -50


@pytest.mark.parametrize(
    "yml",
    [
        "- category: einkaufen\n  match: {}\n",  # leeres match
        "- category: einkaufen\n  match: {party: []}\n",  # leere Patternliste
        "- category: einkaufen\n  match: {party: [42]}\n",  # kein String
        '- category: einkaufen\n  match: {telefon: ["x"]}\n',  # unbekanntes Attribut
        '- category: einkaufen\n  match: {party: ["x"]}\n  foo: 1\n',  # unbekanntes Feld
    ],
)
def test_load_rules_rejects_invalid(tmp_path: Path, yml: str) -> None:
    p = tmp_path / "rules.yml"
    p.write_text(yml, encoding="utf-8")
    with pytest.raises(ValueError):
        load_rules(p)


def test_squashed_matching_repairs_chopped_strings() -> None:
    df = pd.DataFrame(
        {
            "account": ["giro"],
            "party": ["PayPal Europe S.a.r.l."],
            "purpose": ["1031 . ETSY IRELAND UNLIMI TED, Ihr Einkauf bei ETSY I RELAND"],
            "book_text": ["Lastschrift"],
            "amount": [-10.0],
            "merchant": ["etsyirelandunlimitedcompany"],
        }
    )
    rules = [Rule(category="persoenlich", match={"merchant": ("etsy ireland",)})]
    assert apply_rules(df, rules).loc[0, "category"] == "persoenlich"


def test_missing_match_column_yields_no_match_instead_of_keyerror() -> None:
    df = pd.DataFrame(
        {
            "account": ["giro"],
            "party": ["PayPal Europe S.a.r.l."],
            "purpose": ["irgendwas"],
            "book_text": ["Lastschrift"],
            "amount": [-10.0],
            # kein "merchant" — add_norm_columns wurde nicht aufgerufen
        }
    )
    rules = [Rule(category="persoenlich", match={"merchant": ("etsy",)})]
    out = apply_rules(df, rules)
    assert pd.isna(out.loc[0, "category"])


def test_categorize_df_clears_stale_categories() -> None:
    import panda

    df = pd.DataFrame(
        {
            "account": ["giro"],
            "party": ["Unbekannte Party XYZ"],
            "purpose": ["nichts"],
            "book_text": ["Lastschrift"],
            "amount": [-10.0],
            "category": ["freizeit::veraltet"],  # stale aus früherem Lauf
        }
    )
    out = panda.categorize_df(df)
    assert pd.isna(out.loc[0, "category"])
