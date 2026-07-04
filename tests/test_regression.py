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
