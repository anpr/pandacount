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
    # VERIFY: Alte Oldenburger = Andreas?
    "gesundheit::krankenversicherung": "gesundheit::pkv-andreas",
    "gesundheit::krankenzusatz": "gesundheit::zusatz",
    "handy": "persoenlich::handy",
    "haushalt": "wohnen::haushalt",  # ad-hoc category_manual-Wert, siehe Task-5-Report
    "elektronik": "persoenlich::elektronik",  # ad-hoc category_manual-Wert, siehe Task-5-Report
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
    # ad-hoc category_manual-Wert, siehe Task-5-Report
    "moebel::preppen": "wohnen::moebel::preppen",
    "intern::rente": "sparen::wertpapierkauf",
    "restaurant": "freizeit::restaurant",
    "restaurant::urlaub": "urlaub::restaurant",  # ad-hoc category_manual-Wert, siehe Task-5-Report
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
            c for c in distinct if c not in OLD_TO_NEW and c.split("::")[0] not in TOP_LEVELS
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
