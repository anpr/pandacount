# Design: Nachhaltige Transaktions-Kategorisierung & Ein-/Ausgaben-Übersicht

**Datum:** 2026-07-04
**Status:** Entwurf — wartet auf Review durch Andreas

## Ziel

Jährliche und monatliche Übersicht der Ein- und (vor allem) Ausgaben für 2024 und 2025,
auf Basis vollständig genug kategorisierter Transaktionen — und ein Workflow, der dauerhaft
funktioniert, nicht nur für den aktuellen Rückstau.

## Ausgangslage (Stand 2026-07-04)

| Jahr | Transaktionen | unkategorisiert | davon Ausgabenvolumen |
|---|---|---|---|
| 2024 | 2.966 | 1.212 (41%) | −36.771 € |
| 2025 | 3.200 | 1.618 (51%) | −114.351 € |

Strukturelle Befunde:

- **Power-Law-Verteilung:** 1.029 unkategorisierte Parties (2024/25); davon 683 mit genau
  einer Transaktion, aber 37 Parties (>10×) decken 1.213 Transaktionen ab.
  → Regeln lohnen sich für den Kopf, sind für den Long Tail chancenlos.
- **Quick Wins per Regel:** Mayple Tech GmbH (+94,7k, Gehalt Andreas), Ikano Bank (~+101k),
  WEG Holsteinische (−22,2k) — wenige Regeln erledigen den Großteil des Volumens.
- **PayPal-Sonderfall:** 213 Transaktionen; der echte Händler steht im `purpose`
  (mit zerhackten Leerzeichen, z.B. `"Ihr Ei nkauf bei ETSY I RELAND"`).
- `category_manual` existiert im Datenmodell, wurde aber nur 115× benutzt — es fehlt
  ein Werkzeug dafür.
- Gesa-Konto: Kontoauszüge nur bis 30.11.2025 in `downloads/` — Nov/Dez 2025 fehlen
  teilweise; muss beim nächsten Export nachgezogen werden.

## Entscheidungen aus dem Brainstorming

1. **Zielgenauigkeit:** volumen-orientiert. ~95–98% des Ausgabenvolumens kategorisiert;
   der Rest läuft bewusst als „sonstiges“ / `(unkategorisiert)`.
2. **LLM-Einsatz:** kein API-Batch-Skript. Claude Code kategorisiert **interaktiv in
   Sessions** (liest DB, schlägt vor, Andreas bestätigt, Claude schreibt).
3. **Taxonomie:** gezielt aufräumen — feste Top-Level-Liste, Migration per Mapping.
4. **Rhythmus:** quartalsweise / ad-hoc Sessions.
5. **Gewählter Ansatz:** „Regeln als Daten + Claude-Session als Motor“ — jede Session
   erzeugt dauerhaftes Regelwissen statt einmaliger Zuordnungen.

## Abschnitt 1: Taxonomie

**Prinzip:** Geschlossene Liste von Top-Level-Kategorien (Erweiterung nur per bewusster
Entscheidung), zweite/dritte Ebene frei erweiterbar. Trenner bleibt `::`.
Die Top-Level-Ebene ist die Granularität der Monats-/Jahresübersicht.

| Top-Level | Enthält (Beispiele 2. Ebene) | Änderung zu heute |
|---|---|---|
| `einnahmen` | gehalt::andreas, gehalt::gesa, dividende, zinsen, steuererstattung, **kindergeld** | kindergeld von `kinder::` hierher (Einnahme) |
| `wohnen` | rate, wohngeld, strom, internet, putzen, gez, grundsteuer, **moebel** | `moebel::*` → `wohnen::moebel::*` |
| `einkaufen` | Lebensmittel & Drogerie (meist ohne 2. Ebene) | unverändert |
| `kinder` | kleidung, sport, reiten, schule-kita, musik, betreuung | kindergeld raus, sparen raus |
| `mobilitaet` | auto, bahn, oepnv, fliegen, fahrrad, mietwagen, faehre | `autoleihen`→`mietwagen`, `db`/`oebb`→`bahn` |
| `gesundheit` | pkv-andreas, pkv-gesa, vorleistung, apotheke, zusatz | debeka/alte-oldenburger → sprechende Namen |
| `versicherung` | haftpflicht, kfz, hausrat | Tippfehler `hausratversichterung` behoben |
| `freizeit` | restaurant, media, buch, konzert, sport | `restaurant`, `media` werden Sub-Kategorien |
| `urlaub` | unterkunft, einkaufen, freizeit, transport | unverändert |
| `arbeit` | andreas::konferenz, gesa::software, gesa::dienstreise | ersetzt `gesa::arbeit`, `konferenz` |
| `persoenlich` | kleidung, friseur, handy, geschenke | sammelt `kleidung`, `handy`, `geschenk`, `gesa::friseur` |
| `finanzen` | bargeld, anwalt, spenden, mitgliedsbeitraege, gebuehren | sammelt Verstreutes (inkl. justetf) |
| `sparen` | depot-paula, rente-gesa, wertpapierkauf | **neu**: Sparen ist kein Konsum — eigener Block in der Auswertung |

Zusätzlich, außerhalb der Ausgaben-Sicht:

- `intern` und `transfer_category` bleiben unverändert (in Auswertungen ausgeschlossen).
- `sonstiges` ist der explizite Auffang-Eimer für den bewusst ignorierten Long Tail.

**Person-Dimension:** Person nur als Sub-Ebene, wo sie inhaltlich zählt
(`einnahmen::gehalt::gesa`, `arbeit::gesa::…`, `gesundheit::pkv-gesa`).
Kein `gesa::*`-Top-Level mehr.

**Migration:** Einmalige Mapping-Tabelle alt→neu (z.B. `geschenk` →
`persoenlich::geschenke`), angewendet auf (a) die Regeldatei und (b) alle vorhandenen
`category_manual`-Werte in der DB. Details einzelner Sub-Kategorien werden beim Umsetzen
feinjustiert.

## Abschnitt 2: Regelwerk & Normalisierung

### Regeln als Daten

Das hartkodierte Dict `category_attribute_subs_map` in `categorize_df()` (panda.py,
~380 Zeilen) wandert in eine YAML-Datei im Repo (Arbeitstitel `rules.yml`).
Die Matching-Logik bleibt in `panda.py`; nur die Daten ziehen um.

```yaml
- category: freizeit::media
  match:
    party: ["amznprime", "prime video"]
    purpose: ["Spotify AB", "netflix.com"]
- category: freizeit::media
  match: {party: ["VISA APPLE.COM/BILL"]}
  amount_min: -50
- category: einnahmen::gehalt::gesa
  match: {book_text: ["Gehalt/Rente"]}
  account: gesa
```

Semantik exakt wie heute: case-insensitives Substring-Matching, letzte Regel gewinnt,
optionale Einschränkung auf `account`, `amount_min`/`amount_max`, `book_text`.

Begründung: Claude-Sessions (und Andreas selbst) ergänzen Regeln ohne Code-Edit;
`git diff rules.yml` zeigt pro Session sauber, was dazukam; ein Tippfehler bricht nicht
das Skript (Validierung beim Laden, s. Abschnitt 5).

**Escape-Hatch statt DSL:** Die heute existierenden 4 konditionalen Sonderfälle
(Apple < 50 €, Gesa-Gehalt, Smartbroker-Dividende, Finanzamt-Gutschrift) bleiben
einfache Python-Funktionen in `panda.py`. Es wird bewusst **kein** eigenes DSL gebaut —
echtes Python ist der Escape-Hatch für konditionale Logik.
**Offener Punkt:** Sollten die Python-Sonderfälle über die Zeit wuchern, wird die
DSL-Frage neu bewertet (Andreas' Anmerkung aus dem Brainstorming).

### Normalisierung (neuer Schritt vor dem Matching)

Abgeleitete Spalten `party_norm` und `merchant` (Rohdaten und Fingerprints bleiben
unverändert):

- Whitespace-Zerhackung reparieren (ING bricht Strings um: `"ETSY I RELAND"`) —
  Matching auf leerzeichen-insensitiver Variante.
- `VISA `-Präfix strippen; Groß-/Kleinschreibung, Umlaut-Varianten normalisieren.
- **PayPal:** Händler aus `purpose` extrahieren (`"Ihr Einkauf bei X"` → `merchant`);
  Regeln matchen dann auf `merchant` statt auf die PayPal-Party.
- Amazon: `AMAZON PAYMENTS` / `AMAZON EU` konsolidieren.

Effekt: Die 1.029 unkategorisierten Parties schrumpfen deutlich, bevor irgendjemand
manuell arbeitet; bestehende Regellisten (50+ `VISA …`-Restaurant-Einträge) kollabieren.

### Datenmodell

DuckDB unverändert. `category` = Regel-Ergebnis (bei jedem `categorize` neu berechnet),
`category_manual` = menschliche Zuordnung per Fingerprint (gewinnt immer, wird nie
überschrieben). Keine neuen persistierten Spalten nötig.

## Abschnitt 3: Quartals-Workflow (Projekt-Skill)

Dokumentiert als Claude-Code-Skill (`.claude/skills/kategorisieren/SKILL.md`), damit
jede Session gleich abläuft:

1. **Datenlage prüfen:** Neueste CSV-Exports pro IBAN in `downloads/` vs. letztes
   Buchungsdatum pro Konto in der DB → Claude sagt, welche Exporte fehlen
   (genau das Gesa-Nov-2025-Problem). Dann Import + `categorize`.
2. **Kopf zuerst:** Unkategorisiertes gruppiert nach `merchant`/`party_norm`,
   absteigend nach |Volumen|. Für alles ≥2 Vorkommen oder ≥ Schwellwert schlägt Claude
   eine **Regel** vor (Kategorie + Pattern), präsentiert in Batches von ~10.
   Andreas bestätigt/korrigiert; Claude schreibt `rules.yml`, führt `categorize` aus.
3. **Einzelfälle:** Für verbleibende große Einzeltransaktionen schlägt Claude
   `category_manual` vor. Was aus den Daten nicht erschließbar ist (z.B.
   „Tim Leers −12.500 €“), fragt Claude direkt — der Mensch-in-der-Schleife-Teil.
4. **Abschluss mit Messwert:** „Unkategorisiert: X € = Y% des Ausgabenvolumens“ pro
   Jahr. Unter der Schwelle (~2–5%, Andreas entscheidet situativ) ist die Session
   fertig; der Rest bleibt `NULL` und erscheint in der Auswertung als
   `(unkategorisiert)`.

Invarianten: Claude schreibt nie Kategorien ohne Bestätigung (aber Bestätigung in
Batches, nicht pro Transaktion). Alles Bestätigte wird entweder Regel (wiederverwendbar)
oder `category_manual` (fingerprint-stabil, überlebt Re-Importe).

## Abschnitt 4: Auswertung (marimo)

Neuer Übersichts-Teil oben in `panda_analysis.py`:

- **Jahresübersicht** (2024, 2025, …): Einnahmen, Ausgaben nach Top-Level-Kategorie,
  Sparen als separater Block, Saldo. Aufklappbar auf 2. Ebene.
- **Monatsmatrix:** Zeilen = Kategorien, Spalten = Monate, Werte = Summen, inkl.
  Monatsdurchschnitt. Unkategorisiertes als eigene Zeile `(unkategorisiert)`.
- **Datenstand-Warnung:** letztes Buchungsdatum pro Konto — unvollständige Monate
  (Gesa: Nov/Dez 2025) fallen sofort auf, statt Zahlen still zu verfälschen.
- Filter: Jahr, Konto. Transfers und `intern` ausgeschlossen (wie bisher).

Bestehende Spezial-Zellen (Arbeitszimmer, Anwalt, …) bleiben unangetastet.

## Abschnitt 5: Migration, Fehlerfälle, Tests

**Umsetzungsreihenfolge** (jeder Schritt einzeln verifizierbar):

1. Regeln 1:1 nach YAML extrahieren → Regressionstest: Kategorie-Zählungen
   vorher = nachher identisch.
2. Normalisierung + PayPal/Amazon-Extraktion → nur zusätzliche Treffer erlaubt.
3. Taxonomie-Migration (Mapping auf YAML-Regeln und `category_manual`-Werte;
   DuckDB-Backup vorher).
4. Übersichts-Zellen in marimo.
5. Skill-Datei für den Quartals-Workflow.
6. Erste echte Kategorisierungs-Session für 2024/2025 (Rückstau abarbeiten,
   Workflow am echten Fall testen).

**Fehlerfälle:**

- YAML-Validierung beim Laden: unbekanntes Top-Level, leere Pattern-Liste oder
  falscher Typ ⇒ Fehler mit Regel-Angabe. Die Validierung erzwingt die geschlossene
  Top-Level-Liste.
- `category_manual` wird von Regeln nie überschrieben.
- DuckDB-Backup vor der Taxonomie-Migration.
- Fingerprints ändern sich nicht (Normalisierung erzeugt nur abgeleitete Spalten).

**Tests (pytest):**

- Normalisierung: PayPal-Extraktion mit echten zerhackten Strings, VISA-Stripping.
- Regel-Anwendung: Substring/Konto/Betrag-Einschränkungen, „letzte Regel gewinnt“.
- Taxonomie-Validierung (geschlossene Top-Level-Liste).
- Regressionsvergleich Schritt 1 (Dict vs. YAML identisch).

## Bewusst außerhalb des Scopes

- Kein LLM-API-Batch (kann später als Zusatzstufe andocken, ohne Umbau).
- Kein eigenes Regel-DSL (offener Punkt, s.o.).
- Keine Änderung an Import/Fingerprint/DuckDB-Schema.
- Keine Web-/UI-App über marimo hinaus.
