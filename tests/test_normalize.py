import pandas as pd

from normalize import add_norm_columns, extract_merchant, party_norm, squash

PAYPAL = "PayPal Europe S.a.r.l. et Cie S.C.A"
ETSY_PURPOSE = (
    "1031617589907 . ETSY IRELAND UNLIMI TED COMPANY, "
    "Ihr Einkauf bei ETSY I RELAND UNLIMITED COMPANY"
)
DISNEY_PURPOSE = "1041640559268/PP.2107.PP/. DisneyPl us, Ihr Einkauf bei DisneyPlus"
EMPTY_PURPOSE = "1034717227962/PP.3030.PP/. , Ihr Ei nkauf bei"


def test_squash_repairs_chopped_whitespace() -> None:
    assert squash("ETSY I RELAND UNLIMITED") == "etsyirelandunlimited"
    assert "etsyireland" in squash(ETSY_PURPOSE)


def test_party_norm_strips_visa_prefix() -> None:
    assert party_norm("VISA REWE MARKT GMBH") == "rewe markt gmbh"
    assert party_norm("REWE  MARKT") == "rewe markt"
    assert party_norm(float("nan")) == ""


def test_extract_merchant_paypal() -> None:
    assert extract_merchant(PAYPAL, ETSY_PURPOSE) == "etsyirelandunlimitedcompany"
    assert extract_merchant(PAYPAL, DISNEY_PURPOSE) == "disneyplus"


def test_extract_merchant_paypal_empty_falls_back_to_party() -> None:
    assert extract_merchant(PAYPAL, EMPTY_PURPOSE) == party_norm(PAYPAL)


def test_extract_merchant_amazon_consolidates() -> None:
    assert extract_merchant("AMAZON PAYMENTS EUROPE S.C.A.", "irgendwas") == "amazon"
    assert extract_merchant("AMAZON EU S.A R.L., NIEDERLASSUNG DEUTSCHLAND", "x") == "amazon"


def test_extract_merchant_default_is_party_norm() -> None:
    assert extract_merchant("VISA KANT KINO", None) == "kant kino"


def test_add_norm_columns() -> None:
    df = pd.DataFrame({"party": [PAYPAL, "VISA KANT KINO"], "purpose": [DISNEY_PURPOSE, None]})
    out = add_norm_columns(df)
    assert list(out["merchant"]) == ["disneyplus", "kant kino"]
    assert list(out["party_norm"]) == ["paypal europe s.a.r.l. et cie s.c.a", "kant kino"]
