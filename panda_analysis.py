import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def __():
    # Initiales Importieren. Bevor das getan wird: `./panda.py categorize` ausführen!
    import datetime
    import pandas as pd
    import matplotlib.pyplot as plt
    from panda import load_pc, add_cat

    pc = load_pc()
    pc = add_cat(pc)
    return datetime, pd, plt, load_pc, add_cat, pc


@app.cell
def __(pc):
    # Display column information
    print("Columns in dataset:")
    print(pc.columns)
    return


@app.cell
def __(pc, pd):
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
    return df, cols, filtered_df, uncategorized_transactions


@app.cell
def __(uncategorized_transactions):
    # Display uncategorized transactions
    uncategorized_transactions
    return


@app.cell
def __(uncategorized_transactions, plt):
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
    return plot_df,


@app.cell
def __(pc):
    # Gesamteinnahmen 2024
    income_df = pc.loc[
        (pc.book_date.dt.year == 2024) &
        (pc['cat'].isin(['einnahmen::gehalt::andreas', 'einnahmen::gehalt::gesa', 'einnahmen::dividende']))
        ]
    return income_df,


@app.cell
def __(income_df):
    # Display income data
    income_df
    return


@app.cell
def __(pd):
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

    return generate_income_overview,


@app.cell
def __(generate_income_overview, income_df):
    # Generate the income overview
    income_overview_df = generate_income_overview(income_df)
    income_overview_df
    return income_overview_df,


@app.cell
def __(pc):
    # Filter für alle Ausgaben im Jahr 2024
    expenses_df = pc.loc[
        (pc.book_date.dt.year == 2024) &  # Nur Buchungen aus dem Jahr 2024
        (~pc['cat'].str.startswith('intern', na=False)) &  # "intern"-Kategorie ausschließen
        (~pc['cat'].str.startswith('einnahmen', na=False)) &  # "einnahmen"-Kategorie ausschließen
        (pc['transfer_category'].isna()) &  # transfer_category muss NaN sein
        (pc['account'].isin(['giro', 'common', 'gesa']))  # Nur bestimmte Konten berücksichtigen
        ]

    print(f"Total expenses amount: {expenses_df['amount'].sum()}")
    return expenses_df,


@app.cell
def __(expenses_df):
    # Display expenses data
    expenses_df
    return


@app.cell
def __(pd):
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

    return generate_expense_overview,


@app.cell
def __(generate_expense_overview, expenses_df):
    # Generate the expense overview
    expense_overview_df = generate_expense_overview(expenses_df)
    expense_overview_df
    return expense_overview_df,


@app.cell
def __(pc):
    # Giro account positive amounts for 2024
    giro_positive_2024 = pc[(pc.account == "giro") & (pc.amount > 0) & (pc.book_date.dt.year == 2024)]
    giro_positive_2024
    return giro_positive_2024,


@app.cell
def __(pc):
    # Anwaltskosten (Legal costs)
    df_legal = pc
    legal_costs_1 = df_legal[
        df_legal['party'].str.contains('KNH|zirngibl', case=False, na=False) |
        df_legal['purpose'].str.contains('KNH|zirngibl', case=False, na=False)
        ]
    print(f"Minimum book date: {df_legal['book_date'].min()}")
    legal_costs_1
    return df_legal, legal_costs_1,


@app.cell
def __(df_legal):
    # Anwaltskosten #2 (Legal costs #2)
    legal_costs_2 = df_legal[
        df_legal['cat'].str.startswith('anwalt', na=False) |
        df_legal['purpose'].str.contains('luig', case=False, na=False) |
        df_legal['party'].str.contains('liu', case=False, na=False)
        ]
    legal_costs_2
    return legal_costs_2,


@app.cell
def __(pc, datetime):
    # Alle wohnen::putzen Ausgaben für das Jahr 2023 (All cleaning expenses for 2023)
    df_2023 = pc[(pc.account == 'common') &
                 (pc.book_date > datetime.datetime(2023, 2, 1, 0, 0, 0)) &
                 (pc.book_date < datetime.datetime(2024, 2, 1, 0, 0, 0))]
    cleaning_2023 = df_2023[df_2023['cat'] == 'wohnen::putzen']
    cleaning_2023
    return df_2023, cleaning_2023,


@app.cell
def __(pc, datetime):
    # Alle wohnen::putzen Ausgaben für das Jahr 2024 (All cleaning expenses for 2024)
    df_2024 = pc[(pc.account == 'common') &
                 (pc.book_date > datetime.datetime(2024, 2, 1, 0, 0, 0)) &
                 (pc.book_date < datetime.datetime(2025, 2, 1, 0, 0, 0))]
    cleaning_2024 = df_2024[df_2024['cat'] == 'wohnen::putzen']
    cleaning_2024
    return df_2024, cleaning_2024,


@app.cell
def __(pc):
    # Arbeitszimmer 2024: Darlehenszinsen (Home office 2024: Loan interest)
    loan_payments_2024 = pc[
        (pc.book_date.dt.year == 2024) &
        (pc.account == 'common') &
        (pc.purpose.str.contains('Tilgung', case=False, na=False)) &
        pc.purpose.str.contains('Leistung')
    ]
    loan_payments_2024
    return loan_payments_2024,


@app.cell
def __(pc):
    # Arbeitszimmer 2024: Stromkosten (Home office 2024: Electricity costs)
    naturstrom_2024 = pc[pc.party.str.contains('Naturstrom', case=False, na=False) & (pc.book_date.dt.year == 2024)]
    electricity_total = naturstrom_2024.amount.sum()
    print(f"Total electricity costs 2024: {electricity_total}")
    return naturstrom_2024, electricity_total,


@app.cell
def __(pc):
    # Arbeitszimmer 2024: Hausgeld (Home office 2024: Housing fees)
    wohngeld = pc[(pc.cat=='wohnen::wohngeld') & (pc.book_date.dt.year == 2024)]
    housing_fees_total = wohngeld.amount.sum()
    print(f"Total housing fees 2024: {housing_fees_total}")
    return wohngeld, housing_fees_total,


@app.cell
def __(pc):
    # Arbeitszimmer 2024: Grundsteuer (Home office 2024: Property tax)
    grundsteuer = pc[(pc.book_date.dt.year == 2024) &
                     (pc.amount < 0) &
                     (pc.purpose.str.contains('Grundst', case=False, na=False))]
    property_tax_total = grundsteuer.amount.sum()
    print(f"Total property tax 2024: {property_tax_total}")
    return grundsteuer, property_tax_total,


@app.cell
def __(pc):
    # Arbeitszimmer 2024: Telefon Mobil (Home office 2024: Mobile phone)
    # Internet ist auf kontist, und deswegen hier nicht sichtbar
    congstar = pc[(pc.book_date.dt.year == 2024) & (pc.purpose.str.contains('2212684943'))]
    mobile_phone_total = congstar.amount.sum()
    print(f"Total mobile phone costs 2024: {mobile_phone_total}")
    return congstar, mobile_phone_total,


@app.cell
def __():
    # Home office summary 2024
    print("=== Home Office Expenses Summary 2024 ===")
    return


if __name__ == "__main__":
    app.run()