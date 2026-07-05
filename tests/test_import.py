from pathlib import Path

import pytest

import panda


def test_to_raw_df_raises_when_header_never_found(tmp_path: Path) -> None:
    """Old-format CSVs (header 'Buchung;Valuta;...') never satisfy the
    'Buchung;Wertstellungsdatum;Auftraggeber' predicate, so skip_lines_until
    must raise instead of looping forever."""
    file_path = tmp_path / "Umsatzanzeige_DE69500105175402313946_20990101.csv"
    file_path.write_text(
        "Buchung;Valuta;Auftraggeber;Buchungstext;Verwendungszweck;Saldo;Betrag\n"
        "03.06.2025;03.06.2025;REWE MARKT;Lastschrift;Einkauf;1.000,00;-50,25\n",
        encoding="iso-8859-1",
    )

    with pytest.raises(ValueError, match="header not found"):
        panda.to_raw_df(str(file_path))


def test_to_raw_df_parses_current_format(tmp_path: Path) -> None:
    file_path = tmp_path / "Umsatzanzeige_DE69500105175402313946_20990101.csv"
    file_path.write_text(
        "Buchung;Wertstellungsdatum;Auftraggeber/Empfänger;Buchungstext;"
        "Verwendungszweck;Saldo;Währung;Betrag;Währung\n"
        "03.06.2025;03.06.2025;REWE MARKT;Lastschrift;Einkauf;1.000,00;EUR;-50,25;EUR\n",
        encoding="iso-8859-1",
    )

    df = panda.to_raw_df(str(file_path))

    assert len(df) == 1
    assert df.loc[0, "amount"] == -50.25
