# Kategorisierung: Regeln-als-Daten + Session-Workflow — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kategorisierungsregeln aus `panda.py` in eine YAML-Datei auslagern, Normalisierung (PayPal/VISA/Whitespace) einbauen, Taxonomie auf 13 feste Top-Level migrieren, marimo-Übersichten (Jahr/Monat) bauen und den quartalsweisen Claude-Session-Workflow als Skill dokumentieren.

**Architecture:** Logik im Code, Regeln als Daten: `rules.py` lädt/validiert/appliziert `rules.yml` (Semantik: case-insensitives Substring-Matching, letzte Regel gewinnt), `normalize.py` erzeugt abgeleitete Spalten (`party_norm`, `merchant`) mit Whitespace-Squashing. Der alte Dict-Pfad bleibt bis zur Taxonomie-Migration als Regressions-Orakel erhalten. DuckDB-Schema und Fingerprints bleiben unverändert; `category_manual` wird nie überschrieben.

**Tech Stack:** Python 3.13.7 (venv: `.venv/pandacount-3.13.7`), pandas, DuckDB, PyYAML, Typer, pytest (neu), marimo, poetry.

**Maßgebliche Spec:** `docs/superpowers/specs/2026-07-04-kategorisierung-design.html` (gilt für Menschen und Agents).

## Global Constraints

- Python-Interpreter für alle Kommandos: `.venv/pandacount-3.13.7/bin/python` (Repo-Root als cwd).
- Alle Funktionen mit Type-Hints; funktionaler Stil bevorzugt (toolz ok).
- Formatierung: `black <files> --line-length 100`; danach `flake8 <files>` und `mypy <files>` müssen sauber sein.
- `category_manual` wird von keinem Schritt überschrieben (Ausnahme: Taxonomie-Mapping in Task 5, mit DB-Backup).
- Fingerprints und DuckDB-Schema ändern sich nicht; Normalisierung erzeugt nur abgeleitete, nicht persistierte Spalten.
- Vor jeder DB-verändernden Migration: `cp pandacount.duckdb pandacount.duckdb.bak-<datum>`.
- Commits: Conventional Commits; Body beginnt mit `AI: …`; Footer `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`. Nie pushen.
- Die 13+2 Top-Level-Kategorien (Task 5) sind eine geschlossene Liste: `einnahmen, wohnen, einkaufen, kinder, mobilitaet, gesundheit, versicherung, freizeit, urlaub, arbeit, persoenlich, finanzen, sparen` + `intern, sonstiges`.

---

## File Structure

| Datei | Verantwortung | Status |
|---|---|---|
| `panda.py` | CLI (Typer), CSV-Import, DuckDB-I/O, Pipeline-Orchestrierung, Spezialregeln (Python-Escape-Hatch), `set-manual` | modifiziert |
| `rules.py` | `Rule`-Datentyp, `load_rules()` + Validierung, `apply_rules()`, `TOP_LEVELS`, `validate_category()` | neu |
| `normalize.py` | `squash()`, `party_norm()`, `extract_merchant()`, `add_norm_columns()` | neu |
| `rules.yml` | Kategorisierungsregeln (Daten, git-versioniert) | neu (generiert) |
| `scripts/export_rules_to_yaml.py` | Einmal-Skript: Dict → `rules.yml` | neu |
| `scripts/migrate_taxonomy.py` | Einmal-Skript: Mapping alt→neu auf `rules.yml` + `category_manual` | neu |
| `tests/test_rules.py` | Loader-/Matching-Semantik (synthetische Daten) | neu |
| `tests/test_normalize.py` | Squash/PayPal/VISA mit echten Strings | neu |
| `tests/test_regression.py` | Neu vs. Legacy auf echter DB | neu (entfällt in Task 5) |
| `tests/test_taxonomy.py` | Geschlossene Top-Level-Liste, `validate_category` | neu |
| `panda_analysis.py` | marimo: + Datenstand, Jahresübersicht, Monatsmatrix, Drilldown | modifiziert |
| `.claude/skills/kategorisieren/SKILL.md` | Quartals-Session-Workflow | neu |
| `CLAUDE.md` | Verweis auf Spec + neue Architektur/Kommandos | modifiziert |

---

### Task 1: pytest-Infrastruktur + Regel-Dict auf Modulebene

**Files:**
- Modify: `pyproject.toml` (dev-dependency pytest)
- Modify: `panda.py` (Dict aus `categorize_df` auf Modulebene ziehen)
- Test: `tests/test_rules_dict.py`

**Interfaces:**
- Produces: `panda.CATEGORY_RULES_MAP: dict[str, dict[str, list]]` — exakt das bisherige `category_attribute_subs_map` (Werte: Listen aus `str` oder `tuple[str, str]` = `(account, pattern)`), Insertion-Order = Anwendungsreihenfolge. Task 3 hängt davon ab.

- [ ] **Step 1: pytest als Dev-Dependency eintragen**

In `pyproject.toml` unter `[tool.poetry.dev-dependencies]` ergänzen:

```toml
pytest = "^8.3"
```

Run: `poetry install`
Expected: installiert ohne Fehler; `.venv/pandacount-3.13.7/bin/python -c "import pytest"` läuft durch.

- [ ] **Step 2: Failing Test schreiben**

```python
# tests/test_rules_dict.py
from panda import CATEGORY_RULES_MAP


def test_rules_map_is_module_level() -> None:
    assert isinstance(CATEGORY_RULES_MAP, dict)
    assert "wohnen::strom" in CATEGORY_RULES_MAP
    assert "gesa::amazon" in CATEGORY_RULES_MAP
    # Tuple-Einträge (account-gescoped) bleiben erhalten
    assert any(
        isinstance(sub, tuple)
        for sub in CATEGORY_RULES_MAP["gesa::amazon"]["party"]
    )
```

Leere Datei `tests/__init__.py` ist NICHT nötig (pytest rootdir-Discovery reicht).

- [ ] **Step 3: Test laufen lassen — muss fehlschlagen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_rules_dict.py -v`
Expected: FAIL — `ImportError: cannot import name 'CATEGORY_RULES_MAP'`

- [ ] **Step 4: Refactoring — Dict auf Modulebene**

In `panda.py`: Die komplette Definition `category_attribute_subs_map: dict[str, dict] = { … }` (aktuell Zeilen 87–465, innerhalb `categorize_df`) unverändert auf Modulebene direkt über `def categorize_df` verschieben und umbenennen:

```python
CATEGORY_RULES_MAP: dict[str, dict] = {
    "anwalt::centurion": {"party": ["zirngibl", "KNH Rechtsanwaelte"]},
    # … alle bestehenden Einträge 1:1, KEINE inhaltliche Änderung …
}
```

In `categorize_df` die erste Zeile der Schleife anpassen:

```python
    for category, subs_map in CATEGORY_RULES_MAP.items():
```

Kein weiterer Code ändert sich (die 5 Spezialblöcke nach der Schleife bleiben unangetastet).

- [ ] **Step 5: Tests + Werkzeuge laufen lassen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v`
Expected: PASS

Run: `black panda.py --line-length 100 && flake8 panda.py && mypy panda.py`
Expected: keine Fehler (wie vor der Änderung).

- [ ] **Step 6: Verhalten unverändert prüfen**

Run: `./panda.py categorize`
Expected: läuft durch, gleiche Zeilenzahl wie vorher („Stored pandacount.duckdb with N rows in total“; N notieren — Referenz für spätere Tasks).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml poetry.lock panda.py tests/test_rules_dict.py
git commit -m "refactor: hoist category rules dict to module level, add pytest" \
  -m "AI: Vorbereitung Regel-Export; Verhalten unverändert (categorize-Lauf identisch)." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `rules.py` — Rule, load_rules, apply_rules (heutige Semantik)

**Files:**
- Create: `rules.py`
- Test: `tests/test_rules.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) Rule(category: str, match: dict[str, tuple[str, ...]], account: str | None = None, amount_min: float | None = None, amount_max: float | None = None)`
  - `load_rules(path: Path = RULES_PATH) -> list[Rule]` — `RULES_PATH = Path(__file__).parent / "rules.yml"`; strukturelle Validierung, wirft `ValueError` mit Regel-Index + Kategorie.
  - `apply_rules(df: pd.DataFrame, rules: list[Rule]) -> pd.DataFrame` — setzt `df["category"]`; Reihenfolge = Listenreihenfolge, spätere Treffer überschreiben.
- Match-Semantik (wie heute): pro Attribut (`party`, `purpose`, `book_text`) case-insensitives Substring-Matching (`regex=False`) auf `fillna("")`; `account` = Gleichheit; `amount_min`/`amount_max` = exklusive Schranken (`amount > amount_min`, `amount < amount_max`).

- [ ] **Step 1: Failing Tests schreiben**

```python
# tests/test_rules.py
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
        Rule(category="a", match={"purpose": ("spotify",)}),
        Rule(category="b", match={"purpose": ("spotify ab",)}),
    ]
    out = apply_rules(_df(), rules)
    assert out.loc[2, "category"] == "b"


def test_account_constraint() -> None:
    rules = [Rule(category="x", match={"party": ("apple",)}, account="common")]
    out = apply_rules(_df(), rules)
    assert pd.isna(out.loc[1, "category"])  # apple ist auf gesa, nicht common


def test_amount_min_exclusive() -> None:
    rules = [Rule(category="media", match={"party": ("apple",)}, amount_min=-50.0)]
    out = apply_rules(_df(), rules)
    assert out.loc[1, "category"] == "media"  # -9.99 > -50
    out2 = apply_rules(_df(amount=[-20.0, -60.0, -12.0]), rules)
    assert pd.isna(out2.loc[1, "category"])  # -60 < -50


def test_load_rules_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "rules.yml"
    p.write_text(
        '- category: einkaufen\n  match: {party: ["bio company"]}\n'
        '- category: media\n  match: {party: ["apple"]}\n  account: gesa\n  amount_min: -50\n',
        encoding="utf-8",
    )
    rules = load_rules(p)
    assert rules[0] == Rule(category="einkaufen", match={"party": ("bio company",)})
    assert rules[1].account == "gesa" and rules[1].amount_min == -50


@pytest.mark.parametrize(
    "yml",
    [
        '- category: einkaufen\n  match: {}\n',                       # leeres match
        '- category: einkaufen\n  match: {party: []}\n',              # leere Patternliste
        '- category: einkaufen\n  match: {party: [42]}\n',            # kein String
        '- category: einkaufen\n  match: {telefon: ["x"]}\n',         # unbekanntes Attribut
        '- category: einkaufen\n  match: {party: ["x"]}\n  foo: 1\n', # unbekanntes Feld
    ],
)
def test_load_rules_rejects_invalid(tmp_path: Path, yml: str) -> None:
    p = tmp_path / "rules.yml"
    p.write_text(yml, encoding="utf-8")
    with pytest.raises(ValueError):
        load_rules(p)
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_rules.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rules'`

- [ ] **Step 3: `rules.py` implementieren**

```python
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
```

- [ ] **Step 4: Tests laufen lassen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_rules.py -v`
Expected: PASS (alle)

- [ ] **Step 5: Werkzeuge + Commit**

Run: `black rules.py tests/test_rules.py --line-length 100 && flake8 rules.py && mypy rules.py`
Expected: sauber.

```bash
git add rules.py tests/test_rules.py
git commit -m "feat: add rules module (load/validate/apply YAML rules)" \
  -m "AI: Rule-Datentyp, load_rules mit struktureller Validierung, apply_rules mit heutiger Substring-Semantik; synthetische Tests." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Export dict → `rules.yml`, Regression gegen Legacy, Umschalten

**Files:**
- Create: `scripts/export_rules_to_yaml.py`
- Create: `rules.yml` (generiert)
- Modify: `panda.py` (categorize_df → rules-basiert; Legacy behalten)
- Test: `tests/test_regression.py`

**Interfaces:**
- Consumes: `panda.CATEGORY_RULES_MAP` (Task 1), `rules.load_rules/apply_rules` (Task 2).
- Produces:
  - `panda.apply_special_rules(df: pd.DataFrame) -> pd.DataFrame` — die 5 konditionalen Spezialblöcke (Apple < 50 €, Gesa-Gehalt, Andreas-Gehalt, Smartbroker-Dividende, Finanzamt-Gutschrift), unverändert aus dem alten `categorize_df` extrahiert.
  - `panda.categorize_df(df) -> df` = `apply_special_rules(apply_rules(df, load_rules()))`
  - `panda.categorize_df_legacy(df) -> df` = alter Dict-Loop + `apply_special_rules` (Regressions-Orakel, wird in Task 5 gelöscht)
  - `rules.yml` im Repo-Root.

- [ ] **Step 1: Export-Skript schreiben**

```python
# scripts/export_rules_to_yaml.py
"""Einmal-Skript: exportiert panda.CATEGORY_RULES_MAP nach rules.yml.

Reihenfolge bleibt erhalten (Dict-Insertion-Order = Anwendungsreihenfolge).
Tuple-Einträge (account, pattern) werden zu eigenen account-gescopten Regeln.
"""
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from panda import CATEGORY_RULES_MAP  # noqa: E402


def to_rule_dicts(rules_map: dict[str, dict]) -> list[dict]:
    out: list[dict] = []
    for category, attr_map in rules_map.items():
        plain: dict[str, list[str]] = {}
        scoped: dict[str, dict[str, list[str]]] = {}
        for attr, subs in attr_map.items():
            for sub in subs:
                if isinstance(sub, tuple):
                    account, pattern = sub
                    scoped.setdefault(account, {}).setdefault(attr, []).append(pattern)
                else:
                    plain.setdefault(attr, []).append(sub)
        if plain:
            out.append({"category": category, "match": plain})
        for account, match in scoped.items():
            out.append({"category": category, "match": match, "account": account})
    return out


if __name__ == "__main__":
    target = Path(__file__).parent.parent / "rules.yml"
    target.write_text(
        yaml.dump(
            to_rule_dicts(CATEGORY_RULES_MAP),
            allow_unicode=True,
            sort_keys=False,
            width=100,
        ),
        encoding="utf-8",
    )
    print(f"{target} geschrieben")
```

Run: `.venv/pandacount-3.13.7/bin/python scripts/export_rules_to_yaml.py`
Expected: `rules.yml geschrieben`; Datei existiert; `load_rules()` wirft keinen Fehler:
`.venv/pandacount-3.13.7/bin/python -c "from rules import load_rules; print(len(load_rules()), 'Regeln')"` → ~75 Regeln.

- [ ] **Step 2: `panda.py` umbauen — Spezialblöcke extrahieren, Legacy behalten**

In `panda.py`:

1. Neue Funktion `apply_special_rules(df: pd.DataFrame) -> pd.DataFrame` anlegen; die 5 `df.loc[...]`-Blöcke, die aktuell in `categorize_df` NACH der Dict-Schleife stehen (Apple, Gesa-Gehalt, Andreas-Gehalt, Smartbroker, Finanzamt), unverändert hineinschieben; `return df` am Ende.
2. Altes `categorize_df` umbenennen in `categorize_df_legacy`; darin die Spezialblöcke durch `return apply_special_rules(df)` ersetzen (Dict-Schleife bleibt).
3. Neues `categorize_df`:

```python
def categorize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Setzt category anhand rules.yml + Python-Spezialregeln."""
    from rules import apply_rules, load_rules

    return apply_special_rules(apply_rules(df, load_rules()))
```

(Import oben in die Import-Sektion: `from rules import apply_rules, load_rules` — dann ohne lokalen Import.)

- [ ] **Step 3: Regressionstest schreiben**

```python
# tests/test_regression.py
"""Orakel-Test: YAML-Regeln müssen exakt das liefern, was der alte Dict-Pfad lieferte.

Läuft auf der echten DB; wird übersprungen, wenn keine vorhanden ist.
Dieser Test wird in Task 5 (Taxonomie-Migration) durch den Mapping-Vergleich ersetzt
und danach gelöscht.
"""
from pathlib import Path

import pandas as pd
import pytest

import panda

DB = Path("pandacount.duckdb")


@pytest.mark.skipif(not DB.exists(), reason="echte DB benötigt")
def test_yaml_rules_match_legacy_dict() -> None:
    pc = panda.load_pc_from_db()
    legacy = panda.categorize_df_legacy(pc.copy())
    new = panda.categorize_df(pc.copy())
    pd.testing.assert_series_equal(legacy["category"], new["category"])
```

- [ ] **Step 4: Tests laufen lassen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v`
Expected: PASS — insbesondere `test_yaml_rules_match_legacy_dict`. Wenn FAIL: Diff der abweichenden Zeilen ausgeben (`legacy[legacy["category"] != new["category"]]`), Ursache ist fast immer Export-Reihenfolge oder ein Tuple-Eintrag — Export-Skript fixen, NICHT die Semantik in `rules.py` ändern.

- [ ] **Step 5: Kategorien-Zählung vorher/nachher vergleichen**

```bash
.venv/pandacount-3.13.7/bin/python - <<'EOF'
import duckdb
con = duckdb.connect("pandacount.duckdb", read_only=True)
before = con.execute("SELECT category, count(*) n FROM transactions GROUP BY 1 ORDER BY 1").df()
before.to_csv("/tmp/cat_counts_before.csv", index=False)
print(before.n.sum(), "Zeilen,", len(before), "Kategorien")
EOF
./panda.py categorize
.venv/pandacount-3.13.7/bin/python - <<'EOF'
import duckdb, pandas as pd
con = duckdb.connect("pandacount.duckdb", read_only=True)
after = con.execute("SELECT category, count(*) n FROM transactions GROUP BY 1 ORDER BY 1").df()
before = pd.read_csv("/tmp/cat_counts_before.csv")
pd.testing.assert_frame_equal(before.reset_index(drop=True), after.reset_index(drop=True))
print("identisch ✓")
EOF
```

Expected: `identisch ✓`

- [ ] **Step 6: Werkzeuge + Commit**

Run: `black panda.py scripts/export_rules_to_yaml.py tests/test_regression.py --line-length 100 && flake8 panda.py rules.py && mypy panda.py rules.py`
Expected: sauber.

```bash
git add rules.yml scripts/export_rules_to_yaml.py panda.py tests/test_regression.py
git commit -m "feat: categorize via rules.yml, keep legacy dict as regression oracle" \
  -m "AI: Dict nach rules.yml exportiert (Reihenfolge erhalten), Spezialblöcke als apply_special_rules extrahiert, Regressionstest auf echter DB grün, DB-Kategorien-Zählung identisch." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Normalisierung — squash-Matching, `merchant`, `party_norm`

**Files:**
- Create: `normalize.py`
- Modify: `rules.py` (`_rule_mask` matcht auf gesquashten Werten; `merchant` als Match-Attribut)
- Modify: `panda.py` (`categorize_pipeline` + `add_norm_columns`)
- Test: `tests/test_normalize.py`
- Modify: `tests/test_regression.py` (Gains-only statt Gleichheit)

**Interfaces:**
- Consumes: `rules.apply_rules` (Task 2/3).
- Produces:
  - `normalize.squash(text: str) -> str` — lowercase + ALLE Whitespaces entfernt (repariert ING-Zeilenumbrüche wie `"ETSY I RELAND"`).
  - `normalize.party_norm(party: object) -> str` — lowercase, führendes `"visa "` entfernt, Whitespace kollabiert; `""` für NaN.
  - `normalize.extract_merchant(party: object, purpose: object) -> str` — PayPal: gesquashter Händler nach `"ihreinkaufbei"`; Amazon-Parties → `"amazon"`; sonst `party_norm(party)`.
  - `normalize.add_norm_columns(df: pd.DataFrame) -> pd.DataFrame` — fügt Spalten `party_norm` und `merchant` hinzu.
  - `rules.MATCH_ATTRIBUTES = ("party", "purpose", "book_text", "merchant")`; Matching: `squash(pattern) in squash(wert)`.

- [ ] **Step 1: Failing Tests schreiben (mit echten Strings aus der DB)**

```python
# tests/test_normalize.py
import pandas as pd

from normalize import add_norm_columns, extract_merchant, party_norm, squash

PAYPAL = "PayPal Europe S.a.r.l. et Cie S.C.A"
ETSY_PURPOSE = (
    "1031617589907 . ETSY IRELAND UNLIMI TED COMPANY, "
    "Ihr Einkauf bei ETSY I RELAND UNLIMITED COMPANY"
)
DISNEY_PURPOSE = "1041640559268/PP.2107.PP/. DisneyPl us, Ihr Einkauf bei DisneyPlus"
EMPTY_PURPOSE = "1034717227962/PP.3030.PP/. , Ihr Ei nkauf bei"


def test_squash_repairs_chopped_whitespace() -> None:
    assert squash("ETSY I RELAND UNLIMITED") == "etsyirelandunlimited"
    assert "etsyireland" in squash(ETSY_PURPOSE)


def test_party_norm_strips_visa_prefix() -> None:
    assert party_norm("VISA REWE MARKT GMBH") == "rewe markt gmbh"
    assert party_norm("REWE  MARKT") == "rewe markt"
    assert party_norm(float("nan")) == ""


def test_extract_merchant_paypal() -> None:
    assert extract_merchant(PAYPAL, ETSY_PURPOSE) == "etsyirelandunlimitedcompany"
    assert extract_merchant(PAYPAL, DISNEY_PURPOSE) == "disneyplus"


def test_extract_merchant_paypal_empty_falls_back_to_party() -> None:
    assert extract_merchant(PAYPAL, EMPTY_PURPOSE) == party_norm(PAYPAL)


def test_extract_merchant_amazon_consolidates() -> None:
    assert extract_merchant("AMAZON PAYMENTS EUROPE S.C.A.", "irgendwas") == "amazon"
    assert extract_merchant("AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND", "x") == "amazon"


def test_extract_merchant_default_is_party_norm() -> None:
    assert extract_merchant("VISA KANT KINO", None) == "kant kino"


def test_add_norm_columns() -> None:
    df = pd.DataFrame({"party": [PAYPAL, "VISA KANT KINO"], "purpose": [DISNEY_PURPOSE, None]})
    out = add_norm_columns(df)
    assert list(out["merchant"]) == ["disneyplus", "kant kino"]
    assert list(out["party_norm"]) == ["paypal europe s.a.r.l. et cie s.c.a", "kant kino"]
```

- [ ] **Step 2: Tests laufen lassen — müssen fehlschlagen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_normalize.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'normalize'`

- [ ] **Step 3: `normalize.py` implementieren**

```python
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
    if "amazon" in party_sq:
        return "amazon"
    return party_norm(party)


def add_norm_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["party_norm"] = df["party"].map(party_norm)
    df["merchant"] = [
        extract_merchant(p, z) for p, z in zip(df["party"], df["purpose"])
    ]
    return df
```

- [ ] **Step 4: Tests laufen lassen**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_normalize.py -v`
Expected: PASS

- [ ] **Step 5: Matching in `rules.py` auf squash umstellen + `merchant` erlauben**

In `rules.py`:

```python
from normalize import squash

MATCH_ATTRIBUTES = ("party", "purpose", "book_text", "merchant")
```

`_rule_mask` ersetzen:

```python
def _rule_mask(df: pd.DataFrame, rule: Rule) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    for attr, patterns in rule.match.items():
        values = df[attr].fillna("").map(squash)
        for pattern in patterns:
            mask |= values.str.contains(squash(pattern), regex=False)
    if rule.account is not None:
        mask &= df["account"] == rule.account
    if rule.amount_min is not None:
        mask &= df["amount"] > rule.amount_min
    if rule.amount_max is not None:
        mask &= df["amount"] < rule.amount_max
    return mask
```

Zusatz-Test in `tests/test_rules.py` anhängen:

```python
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
```

- [ ] **Step 6: Pipeline erweitern**

In `panda.py`:

```python
from normalize import add_norm_columns

def categorize_pipeline(pc: pd.DataFrame) -> pd.DataFrame:
    print(f"Categorizing {pc.shape[0]} entries...")
    return pipe(pc, transfer_categorize, add_norm_columns, categorize_df)
```

Hinweis: `save_pc_to_db` selektiert Spalten explizit — `party_norm`/`merchant` werden automatisch NICHT persistiert. Nichts weiter nötig.

`categorize_df_legacy` bekommt KEINE Normalisierung (bleibt Orakel für „alte Welt“).

- [ ] **Step 7: Regressionstest auf Gains-only umstellen**

`tests/test_regression.py` — Testfunktion ersetzen durch:

```python
@pytest.mark.skipif(not DB.exists(), reason="echte DB benötigt")
def test_squash_matching_only_gains() -> None:
    """Squash-Matching darf nur NEUE Treffer bringen — nichts verlieren, nichts umkategorisieren."""
    pc = panda.load_pc_from_db()
    legacy = panda.categorize_df_legacy(pc.copy())["category"]
    from normalize import add_norm_columns

    new = panda.categorize_df(add_norm_columns(pc.copy()))["category"]
    both = pd.DataFrame({"legacy": legacy, "new": new})
    lost = both[both.legacy.notna() & both.new.isna()]
    changed = both[both.legacy.notna() & both.new.notna() & (both.legacy != both.new)]
    gained = both[both.legacy.isna() & both.new.notna()]
    assert lost.empty, f"verlorene Kategorisierungen:\n{pc.loc[lost.index]}"
    assert changed.empty, f"umkategorisiert:\n{both.loc[changed.index]}"
    print(f"\n{len(gained)} zusätzliche Treffer durch Normalisierung")
```

Falls `changed` NICHT leer ist: Zeilen ansehen. Ursache ist dann ein Pattern, das durch squashing breiter matcht (z.B. `"bio company"` → `"biocompany"`). Regel in `rules.yml` präzisieren (längeres Pattern), bis `changed` leer ist — Umkategorisierungen sind in diesem Task nicht erlaubt.

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v -s`
Expected: PASS; Anzahl zusätzlicher Treffer wird ausgegeben (notieren fürs Commit-Log).

- [ ] **Step 8: Kategorisieren + Commit**

Run: `./panda.py categorize`
Expected: läuft durch; danach ist die Zahl unkategorisierter Zeilen gesunken.

```bash
git add normalize.py rules.py panda.py tests/test_normalize.py tests/test_rules.py tests/test_regression.py
git commit -m "feat: whitespace-squash matching, PayPal/Amazon merchant extraction" \
  -m "AI: squash-Matching repariert ING-Zerhackung; merchant/party_norm als abgeleitete Spalten; Gains-only-Regression grün (N zusätzliche Treffer)." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Taxonomie-Migration (13 Top-Level, Mapping alt→neu, DB-Backup)

**Files:**
- Modify: `rules.py` (`TOP_LEVELS`, `validate_category`, Validierung in `load_rules`)
- Create: `scripts/migrate_taxonomy.py`
- Modify: `rules.yml` (Kategorien via Skript umgeschrieben)
- Modify: `panda.py` (Spezialregel Apple: `"media"` → `"freizeit::media"`; Legacy-Code löschen)
- Test: `tests/test_taxonomy.py`
- Delete: `tests/test_regression.py`, `tests/test_rules_dict.py` (Orakel entfällt)

**Interfaces:**
- Produces:
  - `rules.TOP_LEVELS: frozenset[str]` = `{"einnahmen", "wohnen", "einkaufen", "kinder", "mobilitaet", "gesundheit", "versicherung", "freizeit", "urlaub", "arbeit", "persoenlich", "finanzen", "sparen", "intern", "sonstiges"}`
  - `rules.validate_category(category: str) -> None` — wirft `ValueError` bei unbekanntem Top-Level.
  - `load_rules` ruft `validate_category` pro Regel auf.
  - `scripts/migrate_taxonomy.py` mit vollständigem `OLD_TO_NEW`-Mapping (unten) — Task 8 (Skill) referenziert die neue Taxonomie.

- [ ] **Step 1: Failing Tests schreiben**

```python
# tests/test_taxonomy.py
from pathlib import Path

import pytest

from rules import TOP_LEVELS, load_rules, validate_category


def test_top_levels_closed_list() -> None:
    assert "wohnen" in TOP_LEVELS and "sonstiges" in TOP_LEVELS
    assert len(TOP_LEVELS) == 15


def test_validate_category() -> None:
    validate_category("wohnen::moebel::bad")  # ok
    with pytest.raises(ValueError):
        validate_category("moebel::bad")  # altes Top-Level


def test_load_rules_enforces_top_levels(tmp_path: Path) -> None:
    p = tmp_path / "rules.yml"
    p.write_text('- category: handy\n  match: {party: ["congstar"]}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="Top-Level"):
        load_rules(p)


def test_repo_rules_yml_is_valid() -> None:
    load_rules()  # echtes rules.yml muss die geschlossene Liste erfüllen
```

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_taxonomy.py -v`
Expected: FAIL — `ImportError: cannot import name 'TOP_LEVELS'`

- [ ] **Step 2: `TOP_LEVELS` + `validate_category` implementieren**

In `rules.py` ergänzen:

```python
TOP_LEVELS: frozenset[str] = frozenset(
    {
        "einnahmen", "wohnen", "einkaufen", "kinder", "mobilitaet",
        "gesundheit", "versicherung", "freizeit", "urlaub", "arbeit",
        "persoenlich", "finanzen", "sparen", "intern", "sonstiges",
    }
)


def validate_category(category: str) -> None:
    top = category.split("::")[0]
    if top not in TOP_LEVELS:
        raise ValueError(
            f"Kategorie {category!r}: Top-Level {top!r} nicht in der geschlossenen "
            f"Liste {sorted(TOP_LEVELS)} (Spec: docs/superpowers/specs/"
            f"2026-07-04-kategorisierung-design.html)"
        )
```

In `_parse_rule` nach der category-Prüfung einfügen: `validate_category(raw["category"])`.

Hinweis: `tests/test_rules.py` und `tests/test_taxonomy.py::test_repo_rules_yml_is_valid` schlagen jetzt fehl, weil Testdaten/rules.yml noch alte Kategorien tragen. `tests/test_rules.py`: Kategorien in den Testregeln auf gültige umstellen (`"einkaufen"`, `"freizeit::media"` statt `"media"`, `"a"`/`"b"` → `"einkaufen"`/`"freizeit"`, `"x"` → `"persoenlich"`). `rules.yml` wird in Step 3–4 migriert.

- [ ] **Step 3: Migrationsskript mit vollständigem Mapping schreiben**

```python
# scripts/migrate_taxonomy.py
"""Einmal-Skript: migriert Kategorien auf die 13+2 Top-Level der Spec.

Wirkt auf (a) rules.yml und (b) category_manual in pandacount.duckdb.
VORHER Backup anlegen: cp pandacount.duckdb pandacount.duckdb.bak-<datum>
"""
import sys
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

# Vollständiges Mapping alter Kategorie-Strings. Kategorien, die gleich bleiben,
# stehen nicht drin. VERIFY-Markierungen: vor dem Lauf per Konto prüfen (s. Step 4).
OLD_TO_NEW: dict[str, str] = {
    "anwalt::centurion": "finanzen::anwalt",
    "bargeld": "finanzen::bargeld",
    "gesa::amazon": "persoenlich::amazon",
    "gesa::dienstreise::unterkunft": "arbeit::gesa::dienstreise",
    "gesa::arbeit::software": "arbeit::gesa::software",
    "gesa::friseur": "persoenlich::friseur",
    "geschenk": "persoenlich::geschenke",
    "gesundheit::debeka": "gesundheit::pkv-gesa",  # VERIFY: Debeka = Gesa?
    "gesundheit::krankenversicherung": "gesundheit::pkv-andreas",  # VERIFY: Alte Oldenburger = Andreas?
    "gesundheit::krankenzusatz": "gesundheit::zusatz",
    "handy": "persoenlich::handy",
    "kleidung": "persoenlich::kleidung",
    "kinder::babysitter": "kinder::betreuung",
    "kinder::kindergeld": "einnahmen::kindergeld",
    "kinder::sparen": "sparen::depot-paula",
    "kinder::schulekita": "kinder::schule-kita",
    "konferenz": "arbeit::andreas::konferenz",
    "justetf": "finanzen::gebuehren",
    "media": "freizeit::media",
    "mitgliedsbeitraege": "finanzen::mitgliedsbeitraege",
    "mobilitaet::autoleihen": "mobilitaet::mietwagen",
    "mobilitaet::deutschlandticket": "mobilitaet::oepnv::deutschlandticket",
    "mobilitaet::db::oebb:": "mobilitaet::bahn::oebb",
    "mobilitaet::db": "mobilitaet::bahn",
    "mobilitaet::oeffentlich": "mobilitaet::oepnv",
    "moebel": "wohnen::moebel",
    "moebel::bad": "wohnen::moebel::bad",
    "moebel::kueche": "wohnen::moebel::kueche",
    "moebel::beleuchtung": "wohnen::moebel::beleuchtung",
    "moebel::geraete": "wohnen::moebel::geraete",
    "intern::rente": "sparen::wertpapierkauf",
    "restaurant": "freizeit::restaurant",
    "rente::gesa": "sparen::rente-gesa",
    "spenden": "finanzen::spenden",
    "versicherung::hausratversichterung": "versicherung::hausrat",
    "wohnen::GEZ": "wohnen::gez",
}


def migrate_rules_yml(path: Path) -> None:
    entries = yaml.safe_load(path.read_text(encoding="utf-8"))
    for entry in entries:
        entry["category"] = OLD_TO_NEW.get(entry["category"], entry["category"])
    path.write_text(
        yaml.dump(entries, allow_unicode=True, sort_keys=False, width=100),
        encoding="utf-8",
    )
    print(f"{path}: Kategorien migriert")


def migrate_category_manual(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        distinct = [
            r[0]
            for r in con.execute(
                "SELECT DISTINCT category_manual FROM transactions "
                "WHERE category_manual IS NOT NULL AND trim(category_manual) != ''"
            ).fetchall()
        ]
        from rules import TOP_LEVELS

        unmapped = [
            c for c in distinct
            if c not in OLD_TO_NEW and c.split("::")[0] not in TOP_LEVELS
        ]
        if unmapped:
            print("!! Unbekannte category_manual-Werte (bleiben unverändert, bitte prüfen):")
            for c in sorted(unmapped):
                print(f"   {c}")
        for old, new in OLD_TO_NEW.items():
            n = con.execute(
                "SELECT count(*) FROM transactions WHERE category_manual = ?", [old]
            ).fetchone()[0]
            if n:
                con.execute(
                    "UPDATE transactions SET category_manual = ? WHERE category_manual = ?",
                    [new, old],
                )
                print(f"   {old} -> {new}: {n} Zeilen")
    finally:
        con.close()


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    migrate_rules_yml(root / "rules.yml")
    migrate_category_manual(root / "pandacount.duckdb")
    print("Fertig. Jetzt: ./panda.py categorize")
```

- [ ] **Step 4: VERIFY-Punkte prüfen (vor dem Lauf!)**

```bash
.venv/pandacount-3.13.7/bin/python - <<'EOF'
import duckdb
con = duckdb.connect("pandacount.duckdb", read_only=True)
print(con.execute("""
  SELECT account, party, count(*) n, round(sum(amount_cents)/100.0,2) eur
  FROM transactions
  WHERE party ILIKE '%debeka%' OR party ILIKE '%oldenburger%' OR party ILIKE '%envivas%'
  GROUP BY 1,2 ORDER BY 1,2
""").df().to_string(index=False))
EOF
```

Expected: Debeka-Zahlungen laufen über Konto `gesa` (⇒ `pkv-gesa` korrekt), Alte Oldenburger über `giro`/Andreas (⇒ `pkv-andreas` korrekt). **Wenn nicht:** Mapping im Skript entsprechend tauschen, bevor es läuft. Ergebnis im Commit-Body dokumentieren.

- [ ] **Step 5: Backup + Migration laufen lassen**

```bash
cp pandacount.duckdb "pandacount.duckdb.bak-$(date +%Y%m%d)"
.venv/pandacount-3.13.7/bin/python scripts/migrate_taxonomy.py
./panda.py categorize
```

Expected: Skript listet migrierte `category_manual`-Zeilen; keine/wenige „Unbekannte Werte“ (falls welche: einzeln entscheiden — meist ad-hoc-Kategorien, die auf `sonstiges::…` oder ein passendes Top-Level gemappt werden; Mapping ergänzen und Skript erneut laufen lassen). `categorize` läuft ohne `ValueError` durch (rules.yml ist jetzt valide).

- [ ] **Step 6: Aggregat-Verifikation (gemappte Alt-Zählung == Neu-Zählung)**

```bash
.venv/pandacount-3.13.7/bin/python - <<'EOF'
import pandas as pd
import panda
from normalize import add_norm_columns
from scripts.migrate_taxonomy import OLD_TO_NEW

pc = panda.load_pc_from_db()
legacy = panda.categorize_df_legacy(pc.copy())["category"]
legacy_mapped = legacy.map(lambda c: OLD_TO_NEW.get(c, c) if isinstance(c, str) else c)
new = panda.categorize_df(add_norm_columns(pc.copy()))["category"]
lc = legacy_mapped.value_counts().sort_index()
nc = new.value_counts().sort_index()
diff = pd.concat([lc, nc], axis=1, keys=["legacy_mapped", "new"]).fillna(0)
diff = diff[diff.legacy_mapped != diff.new]
# Erwartung: Differenzen NUR als Zugewinne durch squash/merchant (new > legacy_mapped)
losses = diff[diff.new < diff.legacy_mapped]
print(diff.to_string() if not diff.empty else "keine Differenzen")
assert losses.empty, f"Verluste gefunden:\n{losses}"
print("Aggregat-Check ✓")
EOF
```

Expected: `Aggregat-Check ✓` (Zugewinne ok, Verluste nicht).
Hinweis: `scripts/` braucht dafür kein `__init__.py` — falls der Import scheitert, `sys.path.insert(0, "scripts")` in den Einzeiler aufnehmen.

- [ ] **Step 7: Legacy-Code + Orakel-Tests löschen, Apple-Spezialregel anpassen**

In `panda.py`:
- `CATEGORY_RULES_MAP` und `categorize_df_legacy` komplett löschen.
- In `apply_special_rules`: `"media"` → `"freizeit::media"` (Apple-Block). Die übrigen 4 Spezialkategorien (`einnahmen::…`) sind bereits valide.

```bash
git rm tests/test_regression.py tests/test_rules_dict.py
```

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v && ./panda.py categorize`
Expected: alle Tests PASS; categorize läuft.

- [ ] **Step 8: Werkzeuge + Commit**

Run: `black panda.py rules.py scripts/migrate_taxonomy.py tests/test_taxonomy.py --line-length 100 && flake8 panda.py rules.py && mypy panda.py rules.py`
Expected: sauber.

```bash
git add -A rules.yml rules.py panda.py scripts/migrate_taxonomy.py tests/
git commit -m "feat: migrate taxonomy to 13 fixed top-level categories" \
  -m "AI: OLD_TO_NEW-Mapping auf rules.yml + category_manual (Backup angelegt), geschlossene Top-Level-Liste in load_rules erzwungen, Legacy-Dict entfernt. VERIFY-Ergebnis PKV: <hier eintragen>." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: `set-manual`-Kommando

**Files:**
- Modify: `panda.py`
- Test: `tests/test_set_manual.py`

**Interfaces:**
- Consumes: `rules.validate_category` (Task 5), `panda.get_db_path`, DuckDB-Schema.
- Produces: CLI `./panda.py set-manual <FINGERPRINT> <CATEGORY>` — setzt `category_manual`, validiert Kategorie gegen `TOP_LEVELS`. Der Session-Skill (Task 8) benutzt genau dieses Kommando.

- [ ] **Step 1: Failing Test schreiben**

```python
# tests/test_set_manual.py
import os
from pathlib import Path

import duckdb
import pytest

import panda


@pytest.fixture()
def tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)  # get_db_path() ist cwd-relativ
    db = tmp_path / "pandacount.duckdb"
    con = duckdb.connect(str(db))
    panda.create_tables(con)
    con.execute(
        "INSERT INTO transactions (transaction_id, account, book_date, valuta_date, "
        "party, book_text, purpose, amount_cents, fingerprint) "
        "VALUES (1, 'giro', DATE '2025-01-02', DATE '2025-01-02', "
        "'Tim Leers', 'Überweisung', 'Rechnung', -1250000, 'fp-1')"
    )
    con.close()
    return db


def test_set_manual_updates_row(tmp_db: Path) -> None:
    panda.set_manual("fp-1", "wohnen::moebel")
    con = duckdb.connect(str(tmp_db), read_only=True)
    assert con.execute(
        "SELECT category_manual FROM transactions WHERE fingerprint='fp-1'"
    ).fetchone()[0] == "wohnen::moebel"


def test_set_manual_rejects_bad_category(tmp_db: Path) -> None:
    with pytest.raises(ValueError):
        panda.set_manual("fp-1", "quatsch::kategorie")


def test_set_manual_unknown_fingerprint(tmp_db: Path) -> None:
    with pytest.raises(SystemExit):
        panda.set_manual("fp-unbekannt", "wohnen::moebel")
```

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/test_set_manual.py -v`
Expected: FAIL — `AttributeError: module 'panda' has no attribute 'set_manual'`

- [ ] **Step 2: Kommando implementieren**

In `panda.py` (Import oben: `from rules import validate_category` — zusammen mit den bestehenden rules-Imports):

```python
@app.command()
def set_manual(fingerprint: str, category: str) -> None:
    """Setzt category_manual für die Transaktion mit diesem Fingerprint."""
    validate_category(category)
    con = duckdb.connect(str(get_db_path()))
    try:
        n = con.execute(
            "SELECT count(*) FROM transactions WHERE fingerprint = ?", [fingerprint]
        ).fetchone()[0]
        if n == 0:
            typer.echo(f"Kein Eintrag mit Fingerprint {fingerprint}", err=True)
            raise typer.Exit(code=1)
        con.execute(
            "UPDATE transactions SET category_manual = ? WHERE fingerprint = ?",
            [category, fingerprint],
        )
        typer.echo(f"{fingerprint} -> {category}")
    finally:
        con.close()
```

- [ ] **Step 3: Tests + Werkzeuge**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v`
Expected: PASS (`typer.Exit` erbt von `SystemExit` — der dritte Test fängt das).

Run: `black panda.py --line-length 100 && flake8 panda.py && mypy panda.py`
Expected: sauber.

- [ ] **Step 4: Commit**

```bash
git add panda.py tests/test_set_manual.py
git commit -m "feat: add set-manual CLI command with taxonomy validation" \
  -m "AI: category_manual per Fingerprint setzen; Kategorie wird gegen TOP_LEVELS validiert." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: marimo-Übersichten (Datenstand, Jahresübersicht, Monatsmatrix, Drilldown)

**Files:**
- Modify: `panda_analysis.py` (neue Zellen direkt nach der Import-Zelle einfügen, vor den bestehenden Analyse-Zellen)

**Interfaces:**
- Consumes: `panda.load_pc_from_db`, `panda.add_cat` (bestehende erste Zelle liefert `pc` mit Spalte `cat`).
- Produces: nur marimo-Zellen; keine von anderen Tasks konsumierten Symbole.

- [ ] **Step 1: Zellen einfügen**

Direkt nach der ersten Zelle (`import …; pc = load_pc_from_db(); pc = add_cat(pc)`) folgende Zellen einfügen. marimo-Konvention beachten: jede Zelle ist `@app.cell def _(<deps>): … return (<exports>)`; Namen mit `_`-Präfix bleiben zellenlokal.

```python
@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo, pc):
    # Datenstand pro Konto — unvollständige Monate sofort sichtbar machen
    import datetime as _dt

    _stand = pc.groupby("account").book_date.max().sort_values()
    _stale = _stand[_stand < (pc.book_date.max() - _dt.timedelta(days=45))]
    _warn = (
        mo.md(
            "⚠️ **Datenstand-Warnung:** "
            + ", ".join(f"`{a}` endet {d:%d.%m.%Y}" for a, d in _stale.items())
            + " — Zahlen für diese Konten sind in späteren Monaten unvollständig."
        )
        if not _stale.empty
        else mo.md("✅ Alle Konten aktuell.")
    )
    mo.vstack([mo.md("## Datenstand pro Konto"), mo.ui.table(_stand.reset_index()), _warn])
    return


@app.cell
def _(mo, pc):
    # Jahr-Auswahl für die Übersichten
    _years = sorted(pc.book_date.dt.year.unique().tolist())
    year_select = mo.ui.dropdown(
        options={str(y): y for y in _years}, value=str(_years[-1]), label="Jahr"
    )
    year_select
    return (year_select,)


@app.cell
def _(pc, pd):
    def top_level(cat: object) -> str:
        return cat.split("::")[0] if isinstance(cat, str) else "(unkategorisiert)"

    def overview_base(df: pd.DataFrame, year: int) -> pd.DataFrame:
        out = df[
            (df.book_date.dt.year == year)
            & df.transfer_category.isna()
            & df.account.isin(["giro", "gesa", "common"])
        ].copy()
        out["top"] = out["cat"].map(top_level)
        out = out[out["top"] != "intern"]
        out["month"] = out.book_date.dt.month
        return out
    return overview_base, top_level


@app.cell
def _(mo, overview_base, pc, year_select):
    # Jahresübersicht: Einnahmen / Ausgaben nach Top-Level / Sparen / Saldo
    _df = overview_base(pc, year_select.value)
    _einnahmen = _df[_df.top == "einnahmen"].amount.sum()
    _sparen = _df[_df.top == "sparen"].amount.sum()
    _ausgaben_df = _df[~_df.top.isin(["einnahmen", "sparen"])]
    _ausgaben = _ausgaben_df.groupby("top").amount.sum().sort_values()
    _saldo = _df.amount.sum()
    mo.vstack(
        [
            mo.md(
                f"## Jahresübersicht {year_select.value}\n"
                f"**Einnahmen:** {_einnahmen:,.2f} € · "
                f"**Ausgaben:** {_ausgaben.sum():,.2f} € · "
                f"**Sparen:** {_sparen:,.2f} € · **Saldo:** {_saldo:,.2f} €"
            ),
            mo.ui.table(_ausgaben.reset_index().rename(columns={"amount": "summe"})),
        ]
    )
    return


@app.cell
def _(mo, overview_base, pc, pd, year_select):
    # Monatsmatrix: Top-Level × Monat, Ø-Spalte, Saldo-Zeile
    _df = overview_base(pc, year_select.value)
    _matrix = _df.pivot_table(
        index="top", columns="month", values="amount", aggfunc="sum", fill_value=0.0
    )
    _matrix["Ø"] = _matrix.mean(axis=1)
    _order = (
        ["einnahmen"]
        + sorted(
            [t for t in _matrix.index if t not in ("einnahmen", "sparen", "(unkategorisiert)")],
            key=lambda t: _matrix.loc[t, "Ø"],
        )
        + [t for t in ("(unkategorisiert)", "sparen") if t in _matrix.index]
    )
    _matrix = _matrix.reindex(_order)
    _matrix.loc["Saldo"] = _matrix.sum()
    mo.vstack(
        [
            mo.md(f"## Monatsmatrix {year_select.value}"),
            mo.ui.table(_matrix.round(0).reset_index(), page_size=20),
        ]
    )
    return


@app.cell
def _(mo, pc, top_level):
    # Drilldown: 2. Ebene innerhalb eines Top-Levels
    _tops = sorted({top_level(c) for c in pc["cat"].dropna().unique()})
    drill_select = mo.ui.dropdown(options=_tops, value="wohnen", label="Top-Level aufklappen")
    drill_select
    return (drill_select,)


@app.cell
def _(drill_select, mo, overview_base, pc, year_select):
    _df = overview_base(pc, year_select.value)
    _sub = _df[_df.top == drill_select.value].copy()
    _sub["sub"] = _sub["cat"].fillna("(unkategorisiert)")
    _pivot = _sub.pivot_table(
        index="sub", columns="month", values="amount", aggfunc="sum", fill_value=0.0
    )
    _pivot["Σ"] = _pivot.sum(axis=1)
    mo.vstack(
        [
            mo.md(f"### {drill_select.value} — 2. Ebene ({year_select.value})"),
            mo.ui.table(_pivot.round(0).reset_index(), page_size=20),
        ]
    )
    return
```

- [ ] **Step 2: App vollständig ausführen (headless-Verifikation)**

Run: `.venv/pandacount-3.13.7/bin/python -m marimo export html panda_analysis.py -o /tmp/panda_check.html`
Expected: Export läuft ohne Zellen-Fehler durch (alle Zellen werden dabei ausgeführt). Bei Fehlern: Zellnamen-Kollisionen prüfen (marimo verbietet doppelte Top-Level-Namen zwischen Zellen; `_`-Präfix nutzen).

- [ ] **Step 3: Sichtprüfung**

Run: `.venv/pandacount-3.13.7/bin/python -m marimo edit panda_analysis.py` (Andreas oder Executor mit Browser)
Expected: Datenstand-Tabelle zeigt 5 Konten; Gesa-Warnung erscheint (endet 30.11.2025, falls noch nicht nachimportiert); Monatsmatrix zeigt `(unkategorisiert)`-Zeile und `sparen` als letzte Zeile vor Saldo.

- [ ] **Step 4: Commit**

```bash
git add panda_analysis.py
git commit -m "feat: add year/month overview cells to marimo analysis" \
  -m "AI: Datenstand-Warnung pro Konto, Jahresübersicht, Monatsmatrix mit (unkategorisiert)- und sparen-Zeilen, Drilldown auf 2. Ebene." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Session-Skill + CLAUDE.md

**Files:**
- Create: `.claude/skills/kategorisieren/SKILL.md`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: `./panda.py set-manual` (Task 6), `rules.yml`-Format (Task 2/3), Taxonomie (Task 5), Spec-HTML.
- Produces: den dokumentierten Quartals-Workflow; keine Code-Symbole.

- [ ] **Step 1: SKILL.md schreiben**

```markdown
---
name: kategorisieren
description: Quartalsweise Kategorisierungs-Session für pandacount — Datenlage prüfen, unkategorisierte Transaktionen nach Volumen gruppiert vorschlagen (Regeln für Wiederkehrendes, category_manual für Einzelfälle), Andreas bestätigt in Batches. Nutzen bei "kategorisieren", "Kategorisierungs-Session", "unkategorisierte Transaktionen".
---

# Kategorisierungs-Session

Maßgebliche Spec: `docs/superpowers/specs/2026-07-04-kategorisierung-design.html`.

## Invarianten

- NIE eine Kategorie ohne Bestätigung von Andreas schreiben — aber in Batches (~10) bestätigen lassen, nicht pro Transaktion.
- Wiederkehrende Parties (≥2 Vorkommen oder großes Volumen) ⇒ Regel in `rules.yml`.
- Einzelfälle ⇒ `./panda.py set-manual <fingerprint> <category>`.
- Kategorien nur aus der geschlossenen Top-Level-Liste (siehe `rules.TOP_LEVELS`); 2. Ebene frei.
- Ziel ist volumen-orientiert: ~95–98 % des Ausgabenvolumens; Rest bleibt bewusst unkategorisiert.

## Ablauf

### 1. Datenlage prüfen

Pro IBAN neueste CSV in `downloads/` vs. letztes Buchungsdatum in der DB:

    .venv/pandacount-3.13.7/bin/python - <<'EOF'
    import duckdb
    con = duckdb.connect("pandacount.duckdb", read_only=True)
    print(con.execute("SELECT account, max(book_date) FROM transactions GROUP BY 1 ORDER BY 1").df())
    EOF
    ls downloads/Umsatzanzeige_*.csv | sort

Fehlen Exporte: Andreas bitten, sie zu ziehen. Dann `./panda.py ing-import <files>` und `./panda.py categorize`.

### 2. Kopf zuerst: Regel-Kandidaten

    .venv/pandacount-3.13.7/bin/python - <<'EOF'
    import panda
    from normalize import add_norm_columns
    pc = add_norm_columns(panda.add_cat(panda.load_pc_from_db()))
    open_df = pc[pc.cat.isna() & pc.transfer_category.isna()]
    g = (open_df.groupby("merchant")
         .agg(n=("amount", "size"), summe=("amount", "sum"))
         .sort_values("summe", key=abs, ascending=False))
    print(g.head(40).to_string())
    EOF

Für jeden Kandidaten mit n≥2 oder |summe|≥500 €: Kategorie vorschlagen, in Batches ~10 präsentieren
(merchant, n, summe, Beispiel-purpose). Nach Bestätigung: Regel ans Ende von `rules.yml` anhängen
(`- category: …\n  match: {merchant: ["…"]}` bzw. party/purpose), dann `./panda.py categorize`.

### 3. Einzelfälle

Verbleibende große Einzeltransaktionen (|amount| absteigend) mit Kategorie-Vorschlag zeigen;
was aus den Daten nicht erschließbar ist, Andreas direkt fragen.
Bestätigte: `./panda.py set-manual <fingerprint> <category>`.
Fingerprint einer Zeile findet man per:

    SELECT fingerprint, book_date, party, amount_cents/100.0 FROM transactions WHERE …

### 4. Abschluss-Messwert

    SELECT year(book_date) jahr,
           round(sum(CASE WHEN cat_offen AND amount_cents<0 THEN amount_cents ELSE 0 END)/100.0,2) offen_eur,
           round(100.0 * sum(CASE WHEN cat_offen AND amount_cents<0 THEN -amount_cents ELSE 0 END)
                 / nullif(sum(CASE WHEN amount_cents<0 THEN -amount_cents ELSE 0 END),0), 1) offen_pct
    FROM (SELECT *, (coalesce(nullif(trim(category_manual),''), category) IS NULL
                     AND transfer_category IS NULL) AS cat_offen FROM transactions)
    GROUP BY 1 ORDER BY 1;

Unter ~2–5 % (Andreas entscheidet): fertig. Zum Schluss `rules.yml` + `pandacount.duckdb`
committen (git-commit-Skill; Subject `feat: kategorisierungs-session <datum>`).
```

- [ ] **Step 2: CLAUDE.md ergänzen**

In `CLAUDE.md`, nach dem Abschnitt „Project Overview“ einfügen:

```markdown
## Authoritative Spec

`docs/superpowers/specs/2026-07-04-kategorisierung-design.html` ist die maßgebliche Spec für
Kategorisierung, Taxonomie und Auswertung — sie gilt für Menschen UND Agents. Bei Widersprüchen
zwischen diesem File und der Spec gewinnt die Spec.
```

Im Abschnitt „Core Architecture“ die Bullet-Liste ergänzen/anpassen:

```markdown
- **Rules as data**: `rules.yml` (Kategorisierungsregeln, git-versioniert) + `rules.py` (Loader,
  Validierung gegen die geschlossene Top-Level-Liste, Matching) + `normalize.py`
  (party_norm/merchant, Whitespace-Squashing). Konditionale Sonderfälle leben als
  Python-Funktionen in `panda.py` (`apply_special_rules`) — bewusst kein eigenes DSL.
- **Kategorisierungs-Sessions**: quartalsweise via Skill `.claude/skills/kategorisieren/SKILL.md`;
  Einzelfälle per `./panda.py set-manual <fingerprint> <category>`.
```

Unter „Essential Commands“ ergänzen:

```markdown
# Tests
.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v

# Manuelle Einzel-Kategorisierung
./panda.py set-manual <fingerprint> <category>
```

Den Abschnitt „Categorization System“ aktualisieren: Verweis auf `rules.yml` statt „rules in categorize_df()“, und die 13+2 Top-Level auflisten (aus `rules.TOP_LEVELS`).

- [ ] **Step 3: Verifikation**

Run: `.venv/pandacount-3.13.7/bin/python -m pytest tests/ -v`
Expected: PASS (nichts kaputt).
Zusätzlich: SKILL.md-Frontmatter beginnt mit `---` in Zeile 1 (sonst lädt der Skill nicht).

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/kategorisieren/SKILL.md CLAUDE.md
git commit -m "docs: add kategorisieren session skill, point CLAUDE.md at spec" \
  -m "AI: Quartals-Workflow als Skill (Invarianten, 4 Schritte, SQL-Snippets); CLAUDE.md verweist auf die maßgebliche HTML-Spec und die neue Regel-Architektur." \
  -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Erste echte Kategorisierungs-Session 2024/2025 (mit Andreas)

**Files:**
- Modify: `rules.yml` (neue Regeln aus der Session)
- Modify: `pandacount.duckdb` (`category_manual`-Einträge)

**Interfaces:**
- Consumes: alles aus Task 1–8.
- Produces: kategorisierte Daten 2024/2025 unter der Volumen-Schwelle.

**Hinweis: Dieser Task ist Mensch-in-der-Schleife — er kann NICHT von einem Subagenten allein ausgeführt werden. Andreas muss live bestätigen.**

- [ ] **Step 1: Skill ausführen** — `/kategorisieren` in einer Session mit Andreas starten; Ablauf exakt nach `.claude/skills/kategorisieren/SKILL.md`.
- [ ] **Step 2: Bekannte Quick Wins zuerst vorschlagen** (aus der Analyse vom 2026-07-04): Mayple Tech GmbH → `einnahmen::gehalt::andreas` (VERIFY: oder `arbeit::…`? Andreas fragen), Ikano Bank → `einnahmen::zinsen` (VERIFY), WEG Holsteinische −22.196,49 € → Andreas fragen (Sonderumlage?), Tim Leers −12.500 € → Andreas fragen, TU Berlin (+4k) → Andreas fragen (Gehalt Gesa?).
- [ ] **Step 3: Abschluss-Messwert prüfen** — offen_pct pro Jahr < 5 % (Ziel ~2 %); mit Andreas abstimmen, ob gut genug.
- [ ] **Step 4: Commit** (rules.yml + DB) wie im Skill beschrieben.
- [ ] **Step 5: marimo-Übersicht öffnen** und die Jahres-/Monatsübersicht 2024 + 2025 gemeinsam ansehen — das ist das eigentliche Deliverable des gesamten Plans.

---

## Self-Review (durchgeführt beim Schreiben)

- **Spec-Abdeckung:** Abschnitt 1–2 (Kontext/Entscheidungen) → informativ; Abschnitt 3 (Pipeline) → Tasks 2–4; Abschnitt 4 (Taxonomie) → Task 5; Abschnitt 5 (Regelwerk+Workflow) → Tasks 2, 3, 8; Abschnitt 6 (Auswertung) → Task 7; Abschnitt 7 (Migration/Tests) → Tasks 1, 3, 5; CLAUDE.md-Verweis (Auftrag) → Task 8; erste Session → Task 9. Offener DSL-Punkt: bewusst nicht im Plan (Spec: out of scope).
- **Platzhalter:** keine — einzige offene Stellen sind explizite VERIFY-Punkte (PKV-Zuordnung Task 5 Step 4, Quick-Win-Kategorien Task 9 Step 2), die nur Andreas/die Daten klären können; sie haben definierte Prüfschritte.
- **Typ-Konsistenz:** `Rule`/`load_rules`/`apply_rules` (Task 2) werden in Task 3–5 mit identischen Signaturen konsumiert; `validate_category` (Task 5) in Task 6; `add_norm_columns` (Task 4) in Task 5 Step 6 und im Skill (Task 8) identisch benutzt; `set_manual` (Task 6) im Skill (Task 8) als CLI referenziert.

---

## Implementation Notes

- **Task 1 (2026-07-05):** Dict-Hoist 1:1 verifiziert (70 Einträge, Reihenfolge/Tuples/Trailing-Colon-Key erhalten). Entscheidung: Annotation bleibt `dict[str, dict]` wie im Plan-Code (Interfaces-Block nannte `dict[str, dict[str, list]]` — rein kosmetisch, Task 3 konsumiert Werte zur Laufzeit). flake8/mypy-Meldungen in panda.py sind vorbestehend, nicht neu.
- **Task 2 (2026-07-05):** rules.py exakt nach Plan. Zusätzlich (Review-getrieben): pandas-stubs als Dev-Dependency (mypy-Gate reproduzierbar), `.flake8` mit max-line-length=100 — bewusste Entscheidung, löst vorbestehenden Konflikt flake8@79 vs black@100. Offen (Minor, fürs Final-Review): pandas-stubs ^3.0 passt nicht zu pandas 2.2.3, besser ~2.2.
- **Task 3 (2026-07-05):** Export 70 Regeln (nicht ~75 — gesa::amazon-Tuples mergen in eine account-Regel). Regression exakt auf Anhieb; DB-Zählung identisch. Reviewer-Hinweis (strukturell, plan-bedingt): Regressions-Orakel hat einen Blindspot, weil die category-Spalte der DB vorbelegt ist — mitigiert durch direkten Dict↔YAML-Abgleich im Review.
- **Task 4 (2026-07-05):** Normalisierung exakt nach Plan + Koordinator-Zusatz: `_rule_mask` guarded fehlende Match-Spalten (df.get, Test persistiert). 22 Gains, 0 Verluste/Umkategorisierungen — keine rules.yml-Anpassung nötig. Offen (Minor, Final-Review): Squashing wird pro Regel neu berechnet (O(Regeln×Zeilen)) — Vorab-Berechnung pro Spalte wäre schneller.
- **Task 5 (2026-07-05):** Migration sauber; PKV-Annahmen datenbestätigt (Debeka=gesa, Alte Oldenburger=giro). 4 unbekannte category_manual-Werte gemappt (haushalt, elektronik, moebel::preppen, restaurant::urlaub→urlaub::restaurant); 1 Zeile 'gesa' bewusst offen → Task-9-Session. Export-Skript gelöscht (Einweck-Zweck erfüllt). Erkenntnis: `category` ist sticky — categorize setzt nur, cleared nie; 1 Orphan (kinder::babysitter, 2022) sichtbar. Datennotiz: kinder::geschenk (12) vs kinder::geschenke (1).
- **Task 6 (2026-07-05):** Plan-Inkonsistenz aufgelöst: typer.Exit erbt nicht von SystemExit (typer 0.15.1/click 8.1.8) → Implementierung wirft SystemExit(1), Plan-Test blieb verbatim; CLI-Exit-Code end-to-end geprüft. Reviewer bestätigte: Downstream (Task 8) konsumiert nur die CLI, Exception-Typ intern unsichtbar.
- **Task 7 (2026-07-05):** Zellen verbatim, Export fehlerfrei, Reviewer reproduzierte alle Checks unabhängig. Übersicht funktioniert als Frühwarnsystem: 2025 zeigt +99,8k unkategorisiert (Mayple/Ikano — genau die Task-9-Kandidaten). Plan-vererbte Minors notiert (einnahmen-Guard, tote Zellparameter).
- **Task 8 (2026-07-05):** SKILL.md verbatim (Frontmatter byte-verifiziert), CLAUDE.md-Sektionen exakt; SQL-Baseline: offen 2024=5,3%, 2025=23,0% des Ausgabenvolumens. Vorbestehende CLAUDE.md-Altlasten (pandacount.yml-Referenzen) fürs Final-Review notiert.
- **Final-Review + Fixwelle (2026-07-05):** Verdict READY (nach Fixes). Sticky-Category-Klasse geschlossen: exakt 3 Alt-Zeilen (freizeit::magic −139€, freizeit::poker −100€, kinder::babysitter→betreuung −35€) nach category_manual gerettet, dann `category`-Reset in categorize_df → category ist jetzt reiner abgeleiteter Cache (Idempotenz-Invariante 0/0/0, testgesichert). Importer-EOF-Guard (alte CSV-Formate hängen nicht mehr endlos). pandas-stubs ~2.2. CLAUDE.md-YAML-Altlasten raus. Bewusst offen: transfer_category-Reset (14 Alt-Zeilen, LOST≠0 → Session-Agenda), Konto-Filter marimo (Spec-§6-Lücke), Fresh-DB-Import-KeyError (vorbestehend), rules.py-Perf (1,6s, ok bis ~10x Regeln).
- **Task 9 / Session 1 (2026-07-05):** Ziel erreicht — offen 2024: 3,4%, 2025: 5,9% des Ausgabenvolumens (von 5,3%/23,0%). Werkzeuge bewährten sich: recoll löste Maler/Ärztin/Vivaticket, Browser-Matching löste 126 Amazon-Txns artikelgenau (dabei zwei Bugs gefunden: AMZN-Party-Varianten fehlten in merchant-Extraktion — gefixt+getestet; €-Beträge der englischen Amazon-UI wurden als deutsches Format geparst). Erkenntnis: Andreas' eigene Amazon-Käufe laufen über ING-Visa 0014 (giro); Gesas über ihre Karte. Offen: Gesa-CSV ab Dez 2025, ältere Jahre, PayPal-Leerhändler.
