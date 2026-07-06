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

### 3. Einzelfälle — mit Evidenz-Schicht

Verbleibende große Einzeltransaktionen (|amount| absteigend) mit Kategorie-Vorschlag zeigen.
Für ungeklärte Posten oberhalb ~50–100 € gilt die Reihenfolge **recoll → Websearch → Andreas fragen**:

1. **recoll (lokale E-Mail-Suche, Thunderbird-Index):**

       RECOLL_CONFDIR=~/.recoll-thunderbird /Users/anpr/.local/bin/recoll -t -n 5 -q '<party-wörter>'

   Query-Verfeinerung bei zu vielen Treffern: Betrag im deutschen Format dazu (`"1.310,50"`)
   oder Datumsfenster (`date:2025-11-01/2025-11-30`). Treffer (Betreff + Datum) sind die Evidenz.
2. **Websearch** nur als Fallback zur Händler-Klassifikation (was verkauft diese GmbH?).
   NIE Namen von Privatpersonen ins Web schicken — die bleiben lokal/bei Andreas.
   Achtung: Websearch klassifiziert den Händler, nicht den Kauf — E-Mail-Evidenz gewinnt
   bei Widerspruch (Beispiel: „Tonhaus Melodie" klingt nach Musikladen, war laut Mail ein Einbauherd).
3. Was danach offen bleibt, Andreas direkt fragen.

Jeder Vorschlag zitiert seine Evidenz (Mail-Betreff + Datum bzw. Suchtreffer), damit Andreas
in Sekunden bestätigen kann. Bestätigte: `./panda.py set-manual <fingerprint> <category>`.
Fingerprint einer Zeile findet man per:

    SELECT fingerprint, book_date, party, amount_cents/100.0 FROM transactions WHERE …

### 4. Abschluss-Messwert

WICHTIG: Der Nenner ist die **Konsum-Basis** — genettete Beträge auf giro/gesa/common, ohne
Transfers, ohne die Top-Level `intern`/`einnahmen`/`sparen`. Alle Beträge (offen wie Basis) werden
gleich genettet (Gutschriften mindern die Kategorie). NICHT `sum(alle Negativen)` als Nenner nehmen —
das enthält Transfers, Stammkapital-Flüsse und extra-Konten und schönt die Quote massiv (Faktor ~3).

    WITH konsum AS (
      SELECT year(book_date) jahr, amount_cents,
             coalesce(nullif(trim(category_manual),''), category) AS cat
      FROM transactions
      WHERE transfer_category IS NULL
        AND account IN ('giro','gesa','common')
        AND split_part(coalesce(coalesce(nullif(trim(category_manual),''), category), ''), '::', 1)
            NOT IN ('intern','einnahmen','sparen')
    )
    SELECT jahr,
           round(sum(CASE WHEN cat IS NULL THEN amount_cents ELSE 0 END)/100.0, 2) offen_eur,
           round(100.0 * sum(CASE WHEN cat IS NULL THEN amount_cents ELSE 0 END)
                 / nullif(sum(amount_cents), 0), 1) offen_pct
    FROM konsum GROUP BY 1 ORDER BY 1;

Das ist dieselbe Basis wie die Ausgaben-Tabelle im Notebook `panda_uebersicht.py`, damit
Session-Messwert und angezeigte Übersicht identisch sind. Unter ~5 % (Andreas entscheidet): fertig.
Zum Schluss `rules.yml` + `pandacount.duckdb` committen
(git-commit-Skill; Subject `feat: kategorisierungs-session <datum>`).
