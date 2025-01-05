#!/usr/bin/env python
from pathlib import Path
from typing import Callable

import numpy as np
import yaml
import pandas as pd
import typer
from toolz import pipe


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
        file_name, lambda line: line.startswith("Buchung;Valuta;Auftraggeber")
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
        raw_df["valuta_date"] = pd.to_datetime(raw_df["Valuta"], dayfirst=True)
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


def categorize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Sets category column of dataframe."""
    category_attribute_subs_map: dict[str, dict] = {
        "anwalt::centurion": {"party": ["zirngibl", "KNH Rechtsanwaelte"]},
        "bargeld": {"party": ["bargeldauszahlung"], "purpose": ["ING Bargeld Ausz"]},
        "einkaufen": {
            "party": [
                "bio company",
                "biobackhaus",
                "edeka",
                "dm-drogerie",
                "steinecke",
                "nah und gut",
                "visa ralf oelmann",
                "combi verbrauchermarkt",
                "tchibo",
                "REWE MARKT",
                "VISA REWE VIKTOR ADLER",
                "VISA LPG BIOMARKT",
                "VISA BILLA DANKT",
                "VISA ALDI GMBH",
                "VISA SUMUP * ADELES CAFE LI",
                "VISA SCHENKE DELIKATESSEN",
                "VISA BUDNI SAGT DANKE",
                "VISA ROSSMANN 2425",
                "VISA SCHENKE EXPRESSMARKT &",
            ],
            "purpose": [
                "KoRo Handels GmbH",
                "KoRo Drogerie GmbH",
                "BIO COMPANY GmbH",
                "gewuerzland",
                "BIO COMPANY SE",
                "Ihr Einkauf bei Flink SE",
            ],
        },
        "einnahmen::dividende": {"purpose": ["dividende", "Smartbroker"]},
        "einnahmen::gehalt::andreas": {"party": ["andreas edmond profous"]},
        "freizeit::buch": {
            "party": ["BUCHHDLG. FERLEMANN", "BUCHHDLG.FERLEMANN+SCHATZER"],
            "purpose": ["Libri GmbH"],
        },
        "freizeit::konzert": {"purpose": ["Eventim AG"]},
        "freizeit": {"party": ["VISA KANT KINO"]},
        "freizeit::sport": {"party": ["Katherine Finger", "ELIXIA"]},
        "gesa::amazon": {
            "party": [("common", "AMAZON PAYMENTS EUROPE"), ("common", "AMAZON EU S.A R.L.")]
        },
        "geschenk": {
            "party": ["VISA SPIELVOGEL", "popsa", "Foto Meyer", "VISA TOYS WORLD"],
            "purpose": ["superiore.de", "geschenk mama", "Marimekko", "SPIELVOGEL"],
        },
        "gesundheit": {
            "party": [
                "ZAHNARZT DR. MUELLER",
                "JOSEPHINEN APOTHEKE",
                "PRAGER APOTHEKE",
                "FORTUNA APOTHEKE",
                "PRAGERAPOTHEKE",
                "VISA PLUSPUNKT APOTHEKE",
                "VISA ZAHNARZT DR MUELLER",
                "VISA ADLER - APOTHEKE INH.",
                "VISA ADLER - APOTHEKE INH.J",
                "VISA APOTHEKE AM ZOB",
                "FALKEN SAMMER DEPPNER",  # Beratung PKV
            ],
            "purpose": ["Center-Apotheke im Minipreis", "SPEICKSHOP", "SHAVING.IE"],
        },
        "gesundheit::debeka": {"party": ["Debeka Kranken-Versicherung-Verein a.G"]},
        "handy": {"party": ["congstar - eine Marke der Telekom Deutschland GmbH"]},
        "kleidung": {
            "party": [
                "VISA MAGAZZINO",
                "Globetrotter",
                "VISA OUTDOORLADEN GMBH",
                "Zalando Payments GmbH",
                "VISA INTERSPORT FINKE",
                "KLINGENTHAL GMBH",
                "MAAS NATUR GMBH GUETERSLOH",
                "Maas Naturwaren GmbH",
                "VISA THINK STORE",
            ],
            "purpose": ["Bestseller Handels B.V"],
        },
        "kinder": {
            "party": [
                "Musikschule City West",
                "KINDER- UND JUGEND-, REIT- UND FAHRVEREIN ZEHLENDORF E.V.",
                "Kinder- und Jugend-, Reit- undFahrverein Zehlendorf e.V.",
                "KINDER- und JUGEND-REIT- und FAHRVEREIN ZEHLENDORF e.V.",
            ],
            "purpose": ["Zoologischer Garten Be", "Kinderschwimmen", "ECO Brotbox GmbH"],
        },
        "kinder::babysitter": {"party": ["Carolina Sgro"]},
        "kinder::kleidung": {
            "purpose": [
                "Kleines Schuhwerk",
                "Kleine Helden",
                "Petit Bateau Kinderbekleidung",
                "finkid GmbH",
                "greenstories KG",
                "VISA KLEINE HELDEN",
            ]
        },
        "kinder::kindergeld": {"party": ["Bundesagentur fur Arbeit - Familienkasse"]},
        "kinder::sparen": {"purpose": ["Sparen Depot Paula", "Sparplan ISIN LU0360863863"]},
        "kinder::sport": {"party": ["VISA REITSPORT-CENTER", "kokitu / Sascha Splettstoesser"]},
        "kinder::schulekita": {
            "party": ["NBH Schoeneberg", "Forderverein", "Finow-Grundschule e.V."],
            "purpose": [
                "Kassenzeichen: 2134900496613 Paula Profous",
                "Beitrag fur die Sprachforderung",
                "Beitrag fuer die Sprachfoerderung"
            ],
        },
        "kinder::theater": {"party": ["Erika Tribbioli"]},
        "kinder::optiker": {"party": ["Damm Brillen", "VISA DAMM-BRILLEN BERLIN"]},
        "kinder::reiten": {"party": ["Reit- und Fahrverein Zehlendorf e.V.", "KFRFZ e.V."]},
        "konferenz": {
            "party": [
                "VISA MOLLIEDECONGRESBALIE",
                "VISA MOL*HOTEL ZUIDERDUIN",
                "VISA IAIA",
                "VISA TALLINNFORUM.ORG",
            ]
        },
        "justetf": {"party": ["justETF GmbH"]},
        "media": {
            "party": [
                "amznprime",
                "prime video",
                "abo lage der nation",
                "aws emea",
                "thalia.de",
                "VISA AUDIBLE.IT",
                "Stiftung Warentest",
            ],
            "purpose": [
                "Spotify AB",
                "audible.de",
                "netflix.com",
                "PP.2107.PP . SPOTIFY, Ihr Einkauf b ei SPOTIFY",
                "PP . DisneyPlus, Ihr Einkau f bei DisneyPlus",
                "Hugendubel Digital GmbH + Co. KG",
                "Zeit Audio Abo",
            ],
        },
        "mitgliedsbeitraege": {
            "party": [
                "Deutscher Hochschulverband",
                "Naturschutzzentrum Okowerk Berlin e.V.",
                "Bundnis 90 / Die GRUNEN",
            ]
        },
        "mobilitaet::auto": {
            "party": [
                "sprint station",
                "visa shell",
                "riller & schnauck",
                "Bundeskasse in Kiel",
                "VISA STOP + GO SYSTEMZENTRA",
                "ARAL AG",
                "Worldline Sweden AB fuer Shell",
                "VISA ARAL STATION",
                "VISA STAR TANKSTELLE",
                "Landeshauptkasse Berlin",
                "VISA ESSO STATION",
            ],
            "purpose": ["CosmosDirekt Kfz Beitrag"],
        },
        "mobilitaet::autoleihen": {
            "party": [
                "VISA ENTERPRISE RENT A CAR",
                "VISA RENTALCARS.COM",
                "VISA SIXT",
                "VISA WWW.AUTOEUROPE.DE",
                "VISA GOLDCAR PISA",
                "VISA AGIP SERVICE-STATION",
            ]
        },
        "mobilitaet::db::oebb:": {"purpose": ["OBB-Personenverkehr AG", "OEBB PV AG"]},
        "mobilitaet::db": {"party": ["DB Vertrieb GmbH"]},
        "mobilitaet::faehre": {"party": ["VISA SCANDLINES DEUTSCHLAND", "VISA DIRECTF", "VISA TT-LINE GMBH & CO. KG"]},
        "mobilitaet::fahrrad": {"party": ["bike market city", "FAHRRADLADEN MEHRINGHOF"]},
        "mobilitaet::fliegen": {
            "party": [
                "RYANAIR",
                "easyJet",
                "eurowings GmbH",
                "VISA LUFTHANSA",
                "VISA FLIGHTS ON BOOKING.COM",
                "VISA SWISS.COM",
                "VISA AUSTRIAN AI",
            ],
            "purpose": [
                "ryanair limited",
                "deutsche lufthansa",
                "Koninklijke Luchtvaart Maatschappij",
            ],
        },
        "mobilitaet::oeffentlich": {
            "party": ["bvg app", "DB Fernverkehr AG"],
            "purpose": ["DB Vertrieb GmbH"],
        },
        "moebel": {"party": ["VISA JALOU CITY GMBH", "JalouCity Heimtextilien", "VISA TYLKO S.A."]},
        "moebel::bad": {"party": ["VISA MOEVE SHOP"]},
        "moebel::kueche": {"party": ["VISA ZETTLE *K-TEK KUCHENAR"]},
        "moebel::beleuchtung": {
            "party": ["visa elektrowaren prediger", "Elektroanlagen-Technik Pockrandt"],
        },
        "moebel::geraete": {"party": ["eShoppen Germany GmbH"]},
        "intern": {"party": ["andreas profous", "profous", "gesa geissler"]},
        "intern::rente": {"purpose": ["Wertpapierkauf"], "book_text": ["Wertpapierkauf"]},
        "intern::steuerklasse": {"purpose": ["Ausgleich Steuerklasse"]},
        "restaurant": {
            "party": [
                "cocolo ramen",
                "VISA RESTAURANT PRATIRIO",
                "VISA LUCA CAFE AM NEUEN SEE",
                "VISA ALTER HAFEN GASTHAUS",
                "VISA YOGI HAUS",
                "HAPPINESSHEART",
                "VISA SUMUP *HAPPINESS-HEAR",
                "VISA SUMUP *HAPPINESSHEART",
                "lieferando.de",
                "VISA INDIA CLUB",
                "RESTAURANT APRIL",
                "VISA RESTAURANT LENZIG",
                "VISA RESTAURANT KOINONIA",
                "VISA RESTAURANT BEL MONDO",
                "RESTAURANT PARACAS",
                "VISA EATAROUND DELIVERY",
                "VISA ZIMT UND ZUCKER",
                "VISA SPC*RESTAURANT BAHADUR",
                "VISA RESTAURANTE CALIBOCCA",
                "VISA SY RESTAURANT",
                "VISA YOGIHAUS",
                "VISA RESTAURANT APRIL",
                "VISA PARKCAFE BERLIN",
                "ADELES CAFE LI",
                "VISA CAFE REST DEL EUROPE",
                "VISA SPC*SHARMA UND VIR GBR",
                "VISA LULA DELI AND GRILL",
                "VISA SUMUP *LIEN & LOAN",
                "VISA TOMASA ZEHLENDORF",
                "VISA SAN MARINO RESTAURANT",
                "VISA TRATTORIA DA NOI",
                "VISA INDIAN PALACE",
                "CAFE KUCHENZEI",
                "VISA JULES GEISBERG",
                "VISA SUMUP *CLAUDIOS ARS V",
                "VISA ANTICA TAVERNA SRL",
                "VISA CAFFETTERIA DEGLI UFFI",
                "VISA ZOO GASTRONOMIE",
                "VISA SUMUP *LIEN LOAN",
                "VISA BAECKEREI UND KONDITOR",
                "VISA ALTES GASTHAUS BERMPOH",
                "VISA LE NAPOLEON",
                "VISA RESTAURANT A TELHA",
                "VISA JAPANESE BISTRO",
                "VISA TOMASA FRIEDENAU",
                "VISA OSTERIA DEL NONNO",
            ],
            "purpose": ["TIAN FU // BERLIN"],
        },
        "rente::gesa": {"party": ["DWS Investment GmbH"]},
        "spenden": {"party": ["Aerzte ohne Grenzen eV", "Arzte ohne Grenzen"]},
        "urlaub::unterkunft": {
            "purpose": ["Airbnb Payments", "airbnb"],
            "party": [
                "VISA BKG*BOOKING.COM HOTEL",
                "VISA AIRBNB",
                "VISA HAMPTON BY HILTON",
                "VISA ACHAT STERNHOTEL BONN",
                "VISA PRECISE RESORT MARINA",
            ],
        },
        "urlaub::einkaufen": {
            "party": [
                "VISA MENY PRAESTOE I/S",
                "VISA CIRCLE K BARSE RUNDDEL",
                "VISA CARREFOUR CONTACT",
                "VISA CONAD",
                "VISA UNICOOP FIRENZE",
                "VISA UNICOOP FI",
                "VISA SUPERMERCATO PAM",
            ]
        },
        "urlaub::freizeit": {
            "party": [
                "VISA KALVEHAVE LABYRINTPARK",
                "VISA DANMARKS BORGCENTER",
                "VISA KLETTERWALD GRUNHEIDE",
            ]
        },
        "versicherung::haftpflicht": {
            "party": ["asspario Versicherungsdienst AG", "ASSPARIO GmbH"]
        },
        "versicherung::hausratversichterung": {
            "party": ["COYA Hausrat", "Getsafe Digital GmbH"],
            "purpose": ["COYA Hausrat"],
        },
        "wohnen": {"purpose": ["Rate, Putzen, Naturstrom", "Ausgleich WEG"]},
        "wohnen::grundsteuer": {"purpose": ["STEUERNR 024/749/07849 GRUNDST"]},
        "wohnen::GEZ": {"party": ["Rundfunk ARD, ZDF, DRadio"]},
        "wohnen::strom": {"party": ["NaturStromHandel GmbH"]},
        "wohnen::putzen": {"party": ["INES BORNEMANN"]},
        "wohnen::rate": {"purpose": ["Rechnung Darl.-Leistung 6070166475"]},
        "wohnen::wohngeld": {"party": ["WEG Holsteinische Strase 43 in 10717 Berlin"]},
    }

    for category, subs_map in category_attribute_subs_map.items():
        for attribute, subs in subs_map.items():
            # This is to avoid the mistake that subs is just a string.
            assert isinstance(subs, list)
            for sub_item in subs:
                if isinstance(sub_item, str):
                    sub = sub_item
                    df.loc[
                        df[attribute].fillna("").str.lower().str.contains(sub.lower(), regex=False),
                        "category",
                    ] = category
                elif isinstance(sub_item, tuple):
                    account, sub = sub_item
                    df.loc[
                        (
                            df[attribute]
                            .fillna("")
                            .str.lower()
                            .str.contains(sub.lower(), regex=False)
                        )
                        & (df.account == account),
                        "category",
                    ] = category
    df.loc[
        (df.party.fillna("").str.lower().str.contains("VISA APPLE.COM/BILL".lower(), regex=False))
        & (df.amount > -50),
        "category",
    ] = "media"

    df.loc[
        (df.account == "gesa") & (df.book_text == "Gehalt/Rente"),
        "category",
    ] = "einnahmen::gehalt::gesa"

    df.loc[
        (df.account == "giro") &
        ((df.party == "Kreuzwerker") | (df.party == "ANDREAS EDMOND PROFOUS")),
        "category",
    ] = "einnahmen::gehalt::andreas"

    # This is necessary because the party might be andreas, so it could be overwritten as internal.
    df.loc[
        (df.account == "giro") &
        (df.purpose.str.contains("Smartbroker", case=False, na=False)) &
        (df.amount > 0),
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


def to_yaml(df: pd.DataFrame) -> str:
    """
    Convert the dataframe to a yaml file.

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
    """
    Convert a yaml file to a dataframe.

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
    if not Path("pandacount.yml").exists():
        return pd.DataFrame()

    with open("pandacount.yml", "r") as f:
        pc = from_yaml(f.read())
    return pc


def save_pc(pc: pd.DataFrame):
    yml = to_yaml(pc)
    with open("pandacount.yml", "w") as f:
        f.write(yml)
    print(f"\nStored pandacount.yml with {pc.shape[0]} rows in total")


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
    return pipe(pc, transfer_categorize, categorize_df)


@app.command()
def ing_import(file_list: list[str]):
    pc = load_pc()
    for file_name in file_list:
        typer.echo(f"Processing {file_name}")
        df = to_raw_df(file_name)
        print(
            f"  Importing dataframe with {df.shape[0]} rows (pandacount currently has {pc.shape[0]} rows)..."
        )
        pc = import_to_pandacount(pc, df)

    pc = categorize_pipeline(pc)
    save_pc(pc)


@app.command()
def categorize():
    pc = load_pc()
    pc = categorize_pipeline(pc)
    save_pc(pc)


if __name__ == "__main__":
    app()
