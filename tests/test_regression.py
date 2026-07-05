"""Gains-only-Orakel: Squash-Matching darf gegenüber dem alten Dict-Pfad nur
ZUSÄTZLICHE Kategorisierungen bringen — nichts verlieren, nichts umkategorisieren.

Läuft auf der echten DB; wird übersprungen, wenn keine vorhanden ist.
Dieser Test wird in Task 5 (Taxonomie-Migration) durch den Mapping-Vergleich ersetzt
und danach gelöscht.

Seit Task 4 matcht die neue Pipeline auf gesquashten Werten (repariert
ING-Zerhackung) und kennt zusätzlich das abgeleitete merchant-Attribut. Das
darf gegenüber dem Orakel nur GEWINNEN (neue Treffer), nichts verlieren und
nichts umkategorisieren — der legacy-Pfad (categorize_df_legacy) bleibt
bewusst ohne Normalisierung.
"""

from pathlib import Path

import pandas as pd
import pytest

import panda

DB = Path("pandacount.duckdb")


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
