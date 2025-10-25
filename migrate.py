#!/usr/bin/env python
"""One-time migration script and YAML backup utilities.

This module contains functions for migrating from YAML to DuckDB
and for creating YAML backups of the database.
"""
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def to_yaml(df: pd.DataFrame) -> str:
    """Convert the dataframe to a yaml file.

    Args:
        df: The dataframe to convert.

    Returns:
        The dataframe as a yaml file.
    """
    yaml_df = df.copy()
    yaml_df["book_date"] = df.book_date.dt.strftime("%Y-%m-%d")
    yaml_df["valuta_date"] = df.valuta_date.dt.strftime("%Y-%m-%d")
    if "category_manual" not in yaml_df.columns:
        yaml_df["category_manual"] = ""
    yml = yaml.dump(
        yaml_df.reset_index().to_dict(orient="records"),
        sort_keys=False,
        width=120,
        indent=2,
        default_flow_style=False,
        allow_unicode=True,
    )
    return yml


def from_yaml(yml: str) -> pd.DataFrame:
    """Convert a yaml file to a dataframe.

    Args:
        yml: The yaml file to convert.

    Returns:
        The yaml file as a dataframe.
    """
    df = pd.DataFrame(yaml.load(yml, yaml.Loader))
    df["book_date"] = pd.to_datetime(df["book_date"])
    df["valuta_date"] = pd.to_datetime(df["valuta_date"])
    df.drop(labels=["index"], axis=1, inplace=True)
    return df


def load_pc() -> pd.DataFrame:
    """Load transactions from YAML file (legacy/backup function)."""
    if not Path("pandacount.yml").exists():
        return pd.DataFrame()

    with open("pandacount.yml", "r") as f:
        pc = from_yaml(f.read())
    return pc


def save_pc(pc: pd.DataFrame):
    """Save transactions to YAML file (backup function)."""
    yml = to_yaml(pc)
    with open("pandacount.yml", "w") as f:
        f.write(yml)
    print(f"\nStored pandacount.yml with {pc.shape[0]} rows in total")


def migrate_yaml_to_duckdb():
    """One-time migration: Load YAML data and save to DuckDB."""
    # Import here to avoid circular dependency
    from panda import save_pc_to_db

    print("Starting migration from YAML to DuckDB...")

    # Load from YAML
    pc = load_pc()
    if pc.empty:
        print("No data found in pandacount.yml")
        return

    print(f"Loaded {len(pc)} rows from pandacount.yml")

    # Ensure required columns exist
    if "transfer_category" not in pc.columns:
        pc["transfer_category"] = None
    if "category" not in pc.columns:
        pc["category"] = None
    if "category_manual" not in pc.columns:
        pc["category_manual"] = None

    # Save to DuckDB (this will use upsert)
    save_pc_to_db(pc)

    # Verify
    from panda import load_pc_from_db
    pc_from_db = load_pc_from_db()
    print(f"Verification: Loaded {len(pc_from_db)} rows from DuckDB")

    if len(pc) == len(pc_from_db):
        print("✓ Migration successful!")
    else:
        print("⚠ Warning: Row counts don't match!")


if __name__ == "__main__":
    migrate_yaml_to_duckdb()
