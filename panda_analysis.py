import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    # Initiales Importieren. Bevor das getan wird: `./panda.py categorize` ausführen!
    import datetime
    import pandas as pd
    import matplotlib.pyplot as plt
    from panda import load_pc_from_db, add_cat

    pc = load_pc_from_db()
    pc = add_cat(pc)
    return datetime, pc, pd, plt


@app.cell
def _(pc):
    # Display column information
    print("Columns in dataset:")
    print(pc.columns)
    return


@app.cell
def _(pc):
    # Finden von nicht-kategorisierten Abbuchungen auf den Konten 'giro', 'gesa' und 'common' im Jahr 2024

    # Kopie des DataFrames erstellen
    df = pc.loc[(pc.book_date.dt.year == 2024) & (pc.transfer_category.isna())]
    df = df.copy()

    # 'amount_type' setzen basierend auf dem Betrag
    df.loc[df['amount'] > 0, 'amount_type'] = 'Gutschrift'
    df.loc[df['amount'] <= 0, 'amount_type'] = 'Abbuchung'

    # Relevante Spalten auswählen
    cols = ['account', 'book_date', 'party', 'purpose', 'amount', 'cat']

    # Filter für Abbuchungen und Konten
    df = df.loc[
        (df['amount_type'].isin(['Abbuchung', 'Gutschrift'])) & (df['account'].isin(['giro', 'gesa', 'common']))
        ][cols].sort_values(by='amount', ascending=True)

    # Nur Einträge ohne Kategorie anzeigen
    filtered_df = df.loc[df['cat'].isna()]
    print(f"Total uncategorized amount: {filtered_df['amount'].sum()}")

    uncategorized_transactions = filtered_df
    return (uncategorized_transactions,)


@app.cell
def _(uncategorized_transactions):
    # Display uncategorized transactions
    uncategorized_transactions
    return


@app.cell
def _(plt, uncategorized_transactions):
    # Innerhalb der nicht-kategorisierten Abbuchungen, kumulative Summe der Top-X Beträge berechnen und plotten

    # Calculate the cumulative sum of the amounts
    plot_df = uncategorized_transactions.sort_values(by='amount', ascending=True).copy()
    plot_df['cumulative_sum'] = plot_df['amount'].cumsum()

    # Plot the cumulative sum
    plt.figure(figsize=(20, 12))
    plt.plot(range(1, len(plot_df) + 1), plot_df['cumulative_sum'])
    plt.xlabel('Number of Items')
    plt.ylabel('Cumulative Sum of Amount')
    plt.title('Cumulative Sum of Top-x Uncategorized Expenses')
    plt.grid(True)
    plt.show()
    return


@app.cell
def _(pc):
    # Gesamteinnahmen 2024
    income_df = pc.loc[
        (pc.book_date.dt.year == 2024) &
        (pc['cat'].isin(['einnahmen::gehalt::andreas', 'einnahmen::gehalt::gesa', 'einnahmen::dividende']))
        ]
    return (income_df,)


@app.cell
def _(income_df):
    # Display income data
    income_df
    return


@app.cell
def _(pd):
    # Einkommensuebersicht 2024
    def generate_income_overview(income_df: pd.DataFrame) -> pd.DataFrame:
        # Sum by category
        category_sum = income_df.groupby('cat')['amount'].sum()

        # Overall sum
        overall_sum = income_df['amount'].sum()

        # Combine results into a DataFrame
        overview_df = category_sum.reset_index().rename(columns={'amount': 'category_sum'})
        overview_df.loc[len(overview_df)] = ['Overall Sum', overall_sum]

        return overview_df
    return (generate_income_overview,)


@app.cell
def _(generate_income_overview, income_df):
    # Generate the income overview
    income_overview_df = generate_income_overview(income_df)
    income_overview_df
    return


@app.cell
def _(pc):
    # Filter für alle Ausgaben im Jahr 2024
    expenses_df = pc.loc[
        (pc.book_date.dt.year == 2024) &  # Nur Buchungen aus dem Jahr 2024
        (~pc['cat'].str.startswith('intern', na=False)) &  # "intern"-Kategorie ausschließen
        (~pc['cat'].str.startswith('einnahmen', na=False)) &  # "einnahmen"-Kategorie ausschließen
        (pc['transfer_category'].isna()) &  # transfer_category muss NaN sein
        (pc['account'].isin(['giro', 'common', 'gesa']))  # Nur bestimmte Konten berücksichtigen
        ]

    print(f"Total expenses amount: {expenses_df['amount'].sum()}")
    return (expenses_df,)


@app.cell
def _(expenses_df):
    # Display expenses data
    expenses_df
    return


@app.cell
def _(pd):
    # Ausgaben nach Kategorie, Betragstyp und Konto gruppieren
    def generate_expense_overview(expenses_df: pd.DataFrame) -> pd.DataFrame:
        # Replace NaN in 'cat' with 'Uncategorized'
        expenses_df = expenses_df.copy()
        expenses_df['cat'] = expenses_df['cat'].fillna('Uncategorized')

        # Sum by category and account
        category_account_sum = expenses_df.groupby(['cat', 'account'])['amount'].sum().unstack(fill_value=0)

        # Sum by category across all accounts
        category_sum = expenses_df.groupby('cat')['amount'].sum()

        # Overall sum across all accounts
        overall_sum = expenses_df['amount'].sum()

        # Combine results into a DataFrame
        overview_df = category_sum.reset_index().rename(columns={'amount': 'category_sum'})
        overview_df['giro'] = overview_df['cat'].map(category_account_sum.get('giro', {}))
        overview_df['gesa'] = overview_df['cat'].map(category_account_sum.get('gesa', {}))
        overview_df['common'] = overview_df['cat'].map(category_account_sum.get('common', {}))

        # Add overall sum as a final row
        overall_row = pd.DataFrame([{
            'cat': 'Overall Sum',
            'category_sum': overall_sum,
            'giro': category_account_sum.loc[:, 'giro'].sum() if 'giro' in category_account_sum else 0,
            'gesa': category_account_sum.loc[:, 'gesa'].sum() if 'gesa' in category_account_sum else 0,
            'common': category_account_sum.loc[:, 'common'].sum() if 'common' in category_account_sum else 0,
        }])

        overview_df = pd.concat([overview_df, overall_row], ignore_index=True)

        return overview_df
    return (generate_expense_overview,)


@app.cell
def _(expenses_df, generate_expense_overview):
    # Generate the expense overview
    expense_overview_df = generate_expense_overview(expenses_df)
    expense_overview_df
    return


@app.cell
def _(pc):
    # Giro account positive amounts for 2024
    giro_positive_2024 = pc[(pc.account == "giro") & (pc.amount > 0) & (pc.book_date.dt.year == 2024)]
    giro_positive_2024
    return


@app.cell
def _(pc):
    # Anwaltskosten (Legal costs)
    df_legal = pc
    legal_costs_1 = df_legal[
        df_legal['party'].str.contains('KNH|zirngibl', case=False, na=False) |
        df_legal['purpose'].str.contains('KNH|zirngibl', case=False, na=False)
        ]
    print(f"Minimum book date: {df_legal['book_date'].min()}")
    legal_costs_1
    return (df_legal,)


@app.cell
def _(df_legal):
    # Anwaltskosten #2 (Legal costs #2)
    legal_costs_2 = df_legal[
        df_legal['cat'].str.startswith('anwalt', na=False) |
        df_legal['purpose'].str.contains('luig', case=False, na=False) |
        df_legal['party'].str.contains('liu', case=False, na=False)
        ]
    legal_costs_2
    return


@app.cell
def _(datetime, pc):
    # Alle wohnen::putzen Ausgaben für das Jahr 2023 (All cleaning expenses for 2023)
    df_2023 = pc[(pc.account == 'common') &
                 (pc.book_date > datetime.datetime(2023, 2, 1, 0, 0, 0)) &
                 (pc.book_date < datetime.datetime(2024, 2, 1, 0, 0, 0))]
    cleaning_2023 = df_2023[df_2023['cat'] == 'wohnen::putzen']
    cleaning_2023
    return


@app.cell
def _(datetime, pc):
    # Alle wohnen::putzen Ausgaben für das Jahr 2024 (All cleaning expenses for 2024)
    df_2024 = pc[(pc.account == 'common') &
                 (pc.book_date > datetime.datetime(2024, 2, 1, 0, 0, 0)) &
                 (pc.book_date < datetime.datetime(2025, 2, 1, 0, 0, 0))]
    cleaning_2024 = df_2024[df_2024['cat'] == 'wohnen::putzen']
    cleaning_2024
    return


@app.cell
def _(pc):
    # Arbeitszimmer 2024: Darlehenszinsen (Home office 2024: Loan interest)
    loan_payments_2024 = pc[
        (pc.book_date.dt.year == 2024) &
        (pc.account == 'common') &
        (pc.purpose.str.contains('Tilgung', case=False, na=False)) &
        pc.purpose.str.contains('Leistung')
    ]
    loan_payments_2024
    return


@app.cell
def _(pc):
    # Arbeitszimmer 2024: Stromkosten (Home office 2024: Electricity costs)
    naturstrom_2024 = pc[pc.party.str.contains('Naturstrom', case=False, na=False) & (pc.book_date.dt.year == 2024)]
    electricity_total = naturstrom_2024.amount.sum()
    print(f"Total electricity costs 2024: {electricity_total}")
    return


@app.cell
def _(pc):
    # Arbeitszimmer 2024: Hausgeld (Home office 2024: Housing fees)
    wohngeld = pc[(pc.cat=='wohnen::wohngeld') & (pc.book_date.dt.year == 2024)]
    housing_fees_total = wohngeld.amount.sum()
    print(f"Total housing fees 2024: {housing_fees_total}")
    return


@app.cell
def _(pc):
    # Arbeitszimmer 2024: Grundsteuer (Home office 2024: Property tax)
    grundsteuer = pc[(pc.book_date.dt.year == 2024) &
                     (pc.amount < 0) &
                     (pc.purpose.str.contains('Grundst', case=False, na=False))]
    property_tax_total = grundsteuer.amount.sum()
    print(f"Total property tax 2024: {property_tax_total}")
    return


@app.cell
def _(pc):
    # Arbeitszimmer 2024: Telefon Mobil (Home office 2024: Mobile phone)
    # Internet ist auf kontist, und deswegen hier nicht sichtbar
    congstar = pc[(pc.book_date.dt.year == 2024) & (pc.purpose.str.contains('2212684943'))]
    mobile_phone_total = congstar.amount.sum()
    print(f"Total mobile phone costs 2024: {mobile_phone_total}")
    return


@app.cell
def _():
    # ============================================================
    # Arbeitszimmer 2025 — vollständige Berechnung für die
    # Steuererklärung 2025 (volles Jahr, inkl. Dezember).
    # Objekt: Holsteinische Str. 43 WE04, 10717 Berlin.
    # ============================================================
    # Konfiguration / Konstanten
    az_area_total_m2 = 110.0
    az_area_office_m2 = 13.0
    az_office_ratio = az_area_office_m2 / az_area_total_m2  # Flächenanteil ~11,82 %

    # Anschaffungskosten -> AfA über 50 Jahre (2 % p.a.); Quelle: Kaufunterlagen,
    # nicht aus den Kontodaten ableitbar.
    az_afa_years = 50
    az_afa_costs = {
        "Kaufsumme": 575_000.00,
        "Maklergebühr": 41_412.00,
        "Notargebühr (netto)": 4_023.87,
        "Grunderwerbssteuer": 34_500.00,
        "Grundbuchamtsgebühr": 1_142.00,
    }

    # Beruflicher Nutzungsanteil für Kommunikation (kein Flächenanteil).
    az_internet_share = 0.70
    az_telefon_share = 0.60

    # Internet Jan-Apr 2025 lief noch über Kontist (nicht in pandacount) -> manuell.
    az_kontist_internet = [-49.99, -50.59, -49.99, -49.99]
    return (
        az_afa_costs,
        az_afa_years,
        az_internet_share,
        az_kontist_internet,
        az_office_ratio,
        az_telefon_share,
    )


@app.cell
def _(az_kontist_internet, pc, pd):
    # Laufende Kosten 2025 aus pandacount (volles Jahr)
    _y = pc.book_date.dt.year == 2025

    def _euro(series: pd.Series) -> pd.Series:
        """Deutsche Beträge '1.234,56' in float umwandeln."""
        return (
            series.str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    # Darlehenszinsen: Zinsanteil aus dem Verwendungszweck extrahieren
    # ("Rechnung Darl.-Leistung ... Tilgung 898,22 Zinsen 140,12").
    _loan = pc[_y & (pc.account == "common") & pc.purpose.str.contains("Darl.-Leistung", na=False)]
    az_darlehenszinsen = _euro(_loan.purpose.str.extract(r"Zinsen\s+([\d.]+,\d{2})")[0]).sum()

    # Stromkosten (Naturstrom), netto inkl. Jahresabrechnungs-Gutschrift.
    az_strom = -pc[_y & pc.party.str.contains("Naturstrom", case=False, na=False)].amount.sum()

    # Hausgeld (inkl. HGA-Gutschrift).
    az_hausgeld = -pc[_y & (pc.cat == "wohnen::wohngeld")].amount.sum()

    # Grundsteuer (4 Quartalsraten).
    az_grundsteuer = -pc[
        _y & (pc.amount < 0) & pc.purpose.str.contains("Grundst", case=False, na=False)
    ].amount.sum()

    # Internet (1&1 ab Mai in pandacount + Kontist Jan-Apr manuell).
    az_internet = -(
        pc[_y & pc.party.str.contains(r"1\+1 Telecom", case=False, na=False, regex=True)].amount.sum()
        + sum(az_kontist_internet)
    )

    # Telefon mobil (fraenk, ab April 2025).
    az_telefon = -pc[_y & pc.party.str.contains("fraenk", case=False, na=False)].amount.sum()
    return (
        az_darlehenszinsen,
        az_grundsteuer,
        az_hausgeld,
        az_internet,
        az_strom,
        az_telefon,
    )


@app.cell
def _(
    az_afa_costs,
    az_afa_years,
    az_darlehenszinsen,
    az_grundsteuer,
    az_hausgeld,
    az_office_ratio,
    az_strom,
    pd,
):
    # Raumkosten 2025 (Flächenanteil 13/110): AfA + laufende Kosten
    _afa_rows = [
        {
            "position": _name,
            "gesamtkosten_2025": _cost / az_afa_years,
            "kommentar": f"AfA, Nutzungsdauer {az_afa_years} Jahre",
        }
        for _name, _cost in az_afa_costs.items()
    ]
    _laufend_rows = [
        {"position": "Darlehenszinsen 2025", "gesamtkosten_2025": az_darlehenszinsen, "kommentar": "volles Jahr"},
        {"position": "Stromkosten 2025 (netto)", "gesamtkosten_2025": az_strom, "kommentar": "volles Jahr"},
        {"position": "Hausgeld 2025", "gesamtkosten_2025": az_hausgeld, "kommentar": "volles Jahr"},
        {"position": "Grundsteuer 2025", "gesamtkosten_2025": az_grundsteuer, "kommentar": "volles Jahr"},
    ]
    az_raumkosten = pd.DataFrame(_afa_rows + _laufend_rows)
    az_raumkosten["raumkosten_2025"] = az_raumkosten["gesamtkosten_2025"] * az_office_ratio
    az_raumkosten_gesamt = az_raumkosten["raumkosten_2025"].sum()
    az_raumkosten
    return az_raumkosten, az_raumkosten_gesamt


@app.cell
def _(az_internet, az_internet_share, az_telefon, az_telefon_share, pd):
    # Internet & Telefon 2025 (beruflicher Nutzungsanteil statt Flächenanteil)
    az_kommunikation = pd.DataFrame(
        [
            {"position": "Internet 2025", "gesamtkosten_2025": az_internet, "anteil": az_internet_share},
            {"position": "Telefon mobil 2025", "gesamtkosten_2025": az_telefon, "anteil": az_telefon_share},
        ]
    )
    az_kommunikation["raumkosten_2025"] = az_kommunikation["gesamtkosten_2025"] * az_kommunikation["anteil"]
    az_kommunikation_gesamt = az_kommunikation["raumkosten_2025"].sum()
    az_kommunikation
    return az_kommunikation, az_kommunikation_gesamt


@app.cell
def _(
    az_kommunikation,
    az_kommunikation_gesamt,
    az_office_ratio,
    az_raumkosten,
    az_raumkosten_gesamt,
):
    # Zusammenfassung Arbeitszimmer 2025
    print("=== Arbeitszimmer 2025 — absetzbare Kosten ===")
    print(f"Flächenanteil Arbeitszimmer: 13 / 110 m² = {az_office_ratio:.4%}\n")
    print(az_raumkosten[["position", "gesamtkosten_2025", "raumkosten_2025", "kommentar"]].to_string(index=False))
    print(f"\nRaumkosten Gesamt 2025: {az_raumkosten_gesamt:,.2f} EUR\n")
    print(az_kommunikation[["position", "gesamtkosten_2025", "anteil", "raumkosten_2025"]].to_string(index=False))
    print(f"Internet/Telefon Raumkosten Gesamt 2025: {az_kommunikation_gesamt:,.2f} EUR\n")
    print(f">>> ARBEITSZIMMER GESAMT ABSETZBAR 2025: {az_raumkosten_gesamt + az_kommunikation_gesamt:,.2f} EUR")
    return


if __name__ == "__main__":
    app.run()
