#!/Users/anpr/.pyenv/versions/pandacount-3.10.0/bin/python
from pathlib import Path
from typing import List, Callable

import yaml
import pandas as pd
import typer
from toolz import pipe


from contextlib import contextmanager


@contextmanager
def skip_lines_until(file_name: str, predicate: Callable[[str], bool]):
    """Skips lines until the predicate is true.
    Then yields the file _including_ the line where the predicate is true."""
    with open(file_name, mode="r", encoding="iso-8859-1") as f:
        pos = f.tell()
        while not predicate(f.readline()):
            pos = f.tell()
        # Go back to the bqeginning of the line
        f.seek(pos)
        yield f


def get_account(file_name: str) -> str:
    stem = Path(file_name).stem
    _, iban, _ = stem.split("_")
    iban_account_map = {"DE97500105175409854125": "common", "DE69500105175402313946": "giro"}

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


def categorize(df: pd.DataFrame) -> pd.DataFrame:
    category_attribute_subs_map = {
        "bargeld": {"party": ["bargeldauszahlung"]},
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
            ],
            "purpose": [
                "KoRo Handels GmbH",
                "KoRo Drogerie GmbH",
                "BIO COMPANY GmbH",
                "gewuerzland",
            ],
        },
        "einnahmen": {"party": ["andreas edmond profous"]},
        "einnahmen::dividende": {"purpose": ["dividende"]},
        "geschenk": {
            "party": ["VISA SPIELVOGEL"],
            "purpose": ["superiore.de", "geschenk mama", "Marimekko"],
        },
        "gesundheit": {
            "party": [
                "ZAHNARZT DR. MUELLER",
                "JOSEPHINEN APOTHEKE",
                "PRAGER APOTHEKE",
                "FORTUNA APOTHEKE",
            ],
            "purpose": ["Center-Apotheke im Minipreis", "SPEICKSHOP", "SHAVING.IE"],
        },
        "kleidung": {"party": ["VISA MAGAZZINO"]},
        "kinder": {
            "party": [
                "Carolina Sgro",
                "Musikschule City West",
                "Erika Tribbioli",
                "KINDER- UND JUGEND-, REIT- UND FAHRVEREIN ZEHLENDORF E.V.",
            ],
            "purpose": ["Zoologischer Garten Be", "Kinderschwimmen"],
        },
        "kinder::sparen": {"purpose": ["Sparen Depot Paula"]},
        "kinder::schule": {"purpose": ["Kassenzeichen: 2134900496613 Paula Profous"]},
        "media": {
            "party": ["amznprime", "prime video", "abo lage der nation", "aws emea", "thalia.de"],
            "purpose": ["Spotify AB", "audible.de", "netflix.com"],
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
            ],
            "purpose": ["CosmosDirekt Kfz Beitrag"],
        },
        "mobilitaet::autoleihen": {"party": ["VISA ENTERPRISE RENT A CAR", "VISA RENTALCARS.COM"]},
        "mobilitaet::fahrrad": {"party": ["bike market city", "FAHRRADLADEN MEHRINGHOF"]},
        "mobilitaet::fliegen": {
            "party": ["RYANAIR"],
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
        "intern": {"party": ["andreas profous"]},
        "intern::rente": {"purpose": ["Wertpapierkauf"], "book_text": ["Wertpapierkauf"]},
        "intern::steuerklasse": {"purpose": ["Ausgleich Steuerklasse"]},
        "restaurant": {
            "party": [
                "cocolo ramen",
                "HAPPINESSHEART",
                "lieferando.de",
                "VISA RESTAURANT LENZIG",
                "VISA RESTAURANT KOINONIA",
                "VISA RESTAURANT BEL MONDO",
                "RESTAURANT PARACAS",
                "VISA EATAROUND DELIVERY",
                "VISA ZIMT UND ZUCKER",
                "VISA SPC*RESTAURANT BAHADUR",
                "VISA RESTAURANTE CALIBOCCA",
                "VISA SY RESTAURANT",
            ]
        },
        "spenden": {"party": ["Aerzte ohne Grenzen eV"]},
        "sport": {"party": ["Katherine Finger"]},
        "urlaub": {"purpose": ["Airbnb Payments", "airbnb"]},
        "wohnen": {"purpose": ["Rate, Putzen, Naturstrom", "Ausgleich WEG"]},
    }

    for category, subs_map in category_attribute_subs_map.items():
        for attribute, subs in subs_map.items():
            for sub in subs:
                df.loc[
                    df[attribute].fillna("").str.lower().str.contains(sub.lower()), "category"
                ] = category

    df.loc[
        df.party.fillna("").str.lower().str.contains("VISA APPLE.COM/BILL".lower())
        & (df.amount > -50),
        "category",
    ] = "media"

    return df


def to_yaml(df: pd.DataFrame) -> str:
    """
    Convert the dataframe to a yaml file.

    Args:
        df: The dataframe to convert.

    Returns:
        The dataframe as a yaml file.
    """
    df["book_date"] = df.book_date.dt.strftime("%Y-%m-%d")
    df["valuta_date"] = df.valuta_date.dt.strftime("%Y-%m-%d")
    yml = yaml.dump(
        df.reset_index().to_dict(orient="records"),
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


def import_to_pandacount(pc: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    pc = pd.concat([pc, df], ignore_index=True)
    pc.drop_duplicates(
        subset=["account", "book_date", "valuta_date", "party", "book_text", "purpose", "amount"],
        inplace=True,
    )
    pc.sort_values(axis=0, by=["book_date", "account", "valuta_date", "party", "purpose"], inplace=True)
    return pc


def main(file_list: List[str]):
    pc = load_pc()
    for file_name in file_list:
        typer.echo(f"Processing {file_name}")
        df = pipe(file_name, to_raw_df, categorize)
        pc = import_to_pandacount(pc, df)

    save_pc(pc)
    print(pc.head())


if __name__ == "__main__":
    typer.run(main)
