#!/usr/bin/env python
from pathlib import Path
from typing import Callable
import hashlib

import duckdb
import numpy as np
import pandas as pd
import typer
from toolz import pipe

from normalize import add_norm_columns
from rules import apply_rules, load_rules, validate_category


from contextlib import contextmanager

app = typer.Typer()


@contextmanager
def skip_lines_until(file_name: str, predicate: Callable[[str], bool]):
    """Skips lines until the predicate is true.
    Then yields the file _including_ the line where the predicate is true."""
    with open(file_name, mode="r", encoding="iso-8859-1") as f:
        pos = f.tell()
        while not predicate(f.readline()):
            pos = f.tell()
        # Go back to the beginning of the line
        f.seek(pos)
        yield f


def get_account(file_name: str) -> str:
    stem = Path(file_name).stem
    _, iban, _ = stem.split("_")
    iban_account_map = {
        "DE97500105175409854125": "common",
        "DE69500105175402313946": "giro",
        "DE27500105175404412327": "gesa",
        "DE18500105175525166237": "extra",
        "DE28500105175544958810": "extra-common",
    }

    return iban_account_map[iban]


def to_raw_df(file_name: str) -> pd.DataFrame:
    with skip_lines_until(
        file_name, lambda line: line.startswith("Buchung;Wertstellungsdatum;Auftraggeber")
    ) as f:
        raw_df = pd.read_csv(f, sep=";", encoding="iso-8859-1")
        raw_df.rename(
            columns={
                "Währung.1": "currency1",
                "Währung": "currency",
                "Auftraggeber/Empfänger": "party",
                "Buchungstext": "book_text",
                "Verwendungszweck": "purpose",
            },
            inplace=True,
        )
        raw_df["book_date"] = pd.to_datetime(raw_df["Buchung"], dayfirst=True)
        raw_df["valuta_date"] = pd.to_datetime(raw_df["Wertstellungsdatum"], dayfirst=True)
        raw_df["amount"] = pd.to_numeric(
            raw_df["Betrag"].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        )
        raw_df["balance"] = pd.to_numeric(
            raw_df["Saldo"].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        )

    raw_df["account"] = get_account(file_name)
    raw_df = raw_df[
        [
            "account",
            "book_date",
            "valuta_date",
            "party",
            "book_text",
            "purpose",
            "amount",
            "balance",
        ]
    ]
    return raw_df


def apply_special_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Sets category column for the conditional special cases that can't be expressed
    as plain substring rules (amount thresholds, book_text equality, combined
    account/party conditions)."""
    df.loc[
        (df.party.fillna("").str.lower().str.contains("VISA APPLE.COM/BILL".lower(), regex=False))
        & (df.amount > -50),
        "category",
    ] = "freizeit::media"

    df.loc[
        (df.account == "gesa") & (df.book_text == "Gehalt/Rente"),
        "category",
    ] = "einnahmen::gehalt::gesa"

    df.loc[
        (df.account == "giro")
        & ((df.party == "Kreuzwerker") | (df.party == "ANDREAS EDMOND PROFOUS")),
        "category",
    ] = "einnahmen::gehalt::andreas"

    # This is necessary because the party might be andreas, so it could be overwritten as internal.
    df.loc[
        (df.account == "giro")
        & (df.purpose.str.contains("Smartbroker", case=False, na=False))
        & (df.amount > 0),
        "category",
    ] = "einnahmen::dividende"

    df.loc[
        (
            df.party.fillna("")
            .str.lower()
            .str.contains("Finanzamt Charlottenburg".lower(), regex=False)
        )
        & (df.book_text == "Gutschrift"),
        "category",
    ] = "einnahmen::steuererstattung"

    return df


def categorize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Setzt category anhand rules.yml + Python-Spezialregeln."""
    return apply_special_rules(apply_rules(df, load_rules()))


def transfer_categorize(df: pd.DataFrame) -> pd.DataFrame:
    """Adds transfer_category column to df."""

    transfer_category_attribute_subs_map = {
        "giro::gesa": {"purpose": ["Ausgleich Steuerklasse"]},
        "giro::common": {
            "purpose": ["Rate, Putzen, Naturstrom", "Ausgleich WEG", "Sparen Depot Paula"]
        },
        "giro::extra": {"purpose": ["giro::extra"]},
    }

    df.loc[(df["amount"] < 0) & (df["account"] == "extra"), "transfer_category"] = "extra::giro"

    for transfer_category, subs_map in transfer_category_attribute_subs_map.items():
        for attribute, subs in subs_map.items():
            for sub in subs:
                df.loc[
                    df[attribute].fillna("").str.lower().str.contains(sub.lower(), regex=False),
                    "transfer_category",
                ] = transfer_category

    return df


def add_cat(df: pd.DataFrame) -> pd.DataFrame:
    """Adds cat column to df. It's the "final" category"""
    # Some values in category_manual are the empty string, some .nan => treat as .nan everywhere
    df["category_manual"] = df["category_manual"].replace(r"^\s*$", np.nan, regex=True)
    df["cat"] = df["category_manual"]
    df["cat"] = df["cat"].where(~df["category_manual"].isna(), df["category"])
    return df.drop(columns=["category", "category_manual"])


# DuckDB Functions


def get_db_path() -> Path:
    """Get the path to the DuckDB database file."""
    return Path("pandacount.duckdb")


def generate_fingerprint(row: pd.Series) -> str:
    """Generate deterministic fingerprint from natural key fields.

    Natural key: account, book_date, valuta_date, party, book_text, purpose, amount_cents
    """
    # Convert date to ISO string for consistency
    book_date_str = row["book_date"].strftime("%Y-%m-%d") if pd.notna(row["book_date"]) else ""
    valuta_date_str = (
        row["valuta_date"].strftime("%Y-%m-%d") if pd.notna(row["valuta_date"]) else ""
    )

    # Handle None/NaN values consistently
    account = str(row["account"]) if pd.notna(row["account"]) else ""
    party = str(row["party"]) if pd.notna(row["party"]) else ""
    book_text = str(row["book_text"]) if pd.notna(row["book_text"]) else ""
    purpose = str(row["purpose"]) if pd.notna(row["purpose"]) else ""
    amount_cents = str(row["amount_cents"]) if pd.notna(row["amount_cents"]) else "0"

    # Combine all fields with separator
    combined = (
        f"{account}|{book_date_str}|{valuta_date_str}|{party}|{book_text}|{purpose}|{amount_cents}"
    )

    # Generate SHA256 hash
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def create_tables(con: duckdb.DuckDBPyConnection):
    """Create the database schema if it doesn't exist."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            account TEXT NOT NULL,
            book_date DATE NOT NULL,
            valuta_date DATE NOT NULL,
            party TEXT,
            book_text TEXT,
            purpose TEXT,
            amount_cents INTEGER NOT NULL,
            balance_cents INTEGER,
            transfer_category TEXT,
            category TEXT,
            category_manual TEXT,
            fingerprint TEXT NOT NULL UNIQUE,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def load_pc_from_db() -> pd.DataFrame:
    """Load transactions from DuckDB database.

    Converts amount_cents and balance_cents back to decimal for pandas compatibility.
    """
    db_path = get_db_path()

    if not db_path.exists():
        return pd.DataFrame()

    con = duckdb.connect(str(db_path))
    try:
        df = con.execute(
            """
            SELECT
                account,
                book_date,
                valuta_date,
                party,
                book_text,
                purpose,
                amount_cents,
                balance_cents,
                transfer_category,
                category,
                category_manual
            FROM transactions
            ORDER BY book_date, account, valuta_date, party, purpose
        """
        ).df()

        # Convert cents back to decimal amounts
        df["amount"] = df["amount_cents"] / 100.0
        df["balance"] = df["balance_cents"] / 100.0

        # Drop the cents columns (keep only decimal versions for pandas)
        df = df.drop(columns=["amount_cents", "balance_cents"])

        return df
    finally:
        con.close()


def save_pc_to_db(pc: pd.DataFrame):
    """Upsert transactions to DuckDB database using fingerprint-based deduplication.

    Inserts new transactions and updates existing ones based on fingerprint.
    """
    db_path = get_db_path()
    con = duckdb.connect(str(db_path))

    try:
        create_tables(con)

        # Prepare dataframe for insertion
        pc_insert = pc.copy()

        # Convert amount and balance to cents (integers)
        pc_insert["amount_cents"] = (pc_insert["amount"] * 100).round().astype("Int64")
        pc_insert["balance_cents"] = (pc_insert["balance"] * 100).round().astype("Int64")

        # Generate fingerprint for each row
        pc_insert["fingerprint"] = pc_insert.apply(generate_fingerprint, axis=1)

        # Get next available transaction_id
        max_id_result = con.execute(
            "SELECT COALESCE(MAX(transaction_id), 0) FROM transactions"
        ).fetchone()
        next_id = max_id_result[0] + 1 if max_id_result else 1

        # Assign transaction_ids (these will be used for new rows only)
        pc_insert["transaction_id"] = range(next_id, next_id + len(pc_insert))

        # Select columns for insertion
        pc_insert = pc_insert[
            [
                "transaction_id",
                "account",
                "book_date",
                "valuta_date",
                "party",
                "book_text",
                "purpose",
                "amount_cents",
                "balance_cents",
                "transfer_category",
                "category",
                "category_manual",
                "fingerprint",
            ]
        ]

        # Upsert using INSERT ON CONFLICT
        con.execute(
            """
            INSERT INTO transactions (
                transaction_id, account, book_date, valuta_date,
                party, book_text, purpose, amount_cents, balance_cents,
                transfer_category, category, category_manual, fingerprint
            )
            SELECT * FROM pc_insert
            ON CONFLICT (fingerprint)
            DO UPDATE SET
                transfer_category = EXCLUDED.transfer_category,
                category = EXCLUDED.category,
                category_manual = EXCLUDED.category_manual,
                balance_cents = EXCLUDED.balance_cents
        """
        )

        row_count = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        print(f"\nStored pandacount.duckdb with {row_count} rows in total")
    finally:
        con.close()


def import_to_pandacount(pc: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    pc = pd.concat([pc, df], ignore_index=True)
    pc.drop_duplicates(
        subset=["account", "book_date", "valuta_date", "party", "book_text", "purpose", "amount"],
        inplace=True,
    )
    pc.sort_values(
        axis=0, by=["book_date", "account", "valuta_date", "party", "purpose"], inplace=True
    )
    return pc


def categorize_pipeline(pc: pd.DataFrame) -> pd.DataFrame:
    print(f"Categorizing {pc.shape[0]} entries...")
    return pipe(pc, transfer_categorize, add_norm_columns, categorize_df)


@app.command()
def ing_import(file_list: list[str]):
    """Import ING bank CSV files."""
    pc = load_pc_from_db()

    for file_name in file_list:
        typer.echo(f"Processing {file_name}")
        df = to_raw_df(file_name)
        print(
            f"  Importing dataframe with {df.shape[0]} rows "
            f"(pandacount currently has {pc.shape[0]} rows)..."
        )
        pc = import_to_pandacount(pc, df)

    pc = categorize_pipeline(pc)
    save_pc_to_db(pc)


@app.command()
def categorize():
    """Re-categorize all transactions."""
    pc = load_pc_from_db()
    pc = categorize_pipeline(pc)
    save_pc_to_db(pc)


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
            raise SystemExit(1)
        con.execute(
            "UPDATE transactions SET category_manual = ? WHERE fingerprint = ?",
            [category, fingerprint],
        )
        typer.echo(f"{fingerprint} -> {category}")
    finally:
        con.close()


if __name__ == "__main__":
    app()
