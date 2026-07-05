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
    assert (
        con.execute("SELECT category_manual FROM transactions WHERE fingerprint='fp-1'").fetchone()[
            0
        ]
        == "wohnen::moebel"
    )


def test_set_manual_rejects_bad_category(tmp_db: Path) -> None:
    with pytest.raises(ValueError):
        panda.set_manual("fp-1", "quatsch::kategorie")


def test_set_manual_unknown_fingerprint(tmp_db: Path) -> None:
    with pytest.raises(SystemExit):
        panda.set_manual("fp-unbekannt", "wohnen::moebel")
