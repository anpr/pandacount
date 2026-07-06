import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import matplotlib.pyplot as plt

    from panda import add_cat, load_pc_from_db

    pc = add_cat(load_pc_from_db())
    return mo, pc, pd, plt


@app.cell
def _(mo, pc):
    _years = sorted(pc.book_date.dt.year.unique().tolist())
    jahr_select = mo.ui.dropdown(
        options={str(y): y for y in _years}, value=str(_years[-1]), label="Jahr"
    )
    jahr_select
    return (jahr_select,)


@app.cell
def _(pc, pd):
    ACCOUNTS = ["giro", "gesa", "common"]
    FARBE_BLAU = "#2a78d6"  # Ausgaben (Magnitude, eine Farbe)
    FARBE_AQUA = "#1baf7a"  # Einnahmen

    MONATSNAMEN = {
        1: "Jan",
        2: "Feb",
        3: "Mär",
        4: "Apr",
        5: "Mai",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Okt",
        11: "Nov",
        12: "Dez",
    }

    def monatsspalten(pivot: pd.DataFrame) -> pd.DataFrame:
        pivot.columns = [MONATSNAMEN.get(c, str(c)) for c in pivot.columns]
        return pivot

    def top_level(cat: object) -> str:
        return cat.split("::")[0] if isinstance(cat, str) else "(unkategorisiert)"

    def euro(x: float) -> str:
        return f"{x:,.0f} €".replace(",", ".")

    def jahres_basis(df: pd.DataFrame, jahr: int) -> pd.DataFrame:
        out = df[
            (df.book_date.dt.year == jahr) & df.transfer_category.isna() & df.account.isin(ACCOUNTS)
        ].copy()
        out["top"] = out["cat"].map(top_level)
        out = out[out["top"] != "intern"]
        out["monat"] = out.book_date.dt.month
        return out

    return FARBE_AQUA, FARBE_BLAU, euro, jahres_basis, monatsspalten, top_level


@app.cell
def _(euro, jahres_basis, jahr_select, mo, pc):
    # Einnahmen: Kategorien in voller Tiefe
    _df = jahres_basis(pc, jahr_select.value)
    _ein = _df[_df.top == "einnahmen"].copy()
    _ein["kategorie"] = _ein["cat"].str.removeprefix("einnahmen::")
    einnahmen_tab = (
        _ein.groupby("kategorie").amount.sum().sort_values(ascending=False).reset_index()
    )
    einnahmen_summe = _ein.amount.sum()
    mo.vstack(
        [
            mo.md(f"## Einnahmen {jahr_select.value} — **{euro(einnahmen_summe)}**"),
            mo.ui.table(einnahmen_tab.round(2), page_size=15),
        ]
    )
    return (einnahmen_summe,)


@app.cell
def _(einnahmen_summe, euro, jahres_basis, jahr_select, mo, pc):
    # Ausgaben nach Top-Level (mit Anteil und Ø/Monat), sparen separat, Saldo
    _df = jahres_basis(pc, jahr_select.value)
    _aus = _df[~_df.top.isin(["einnahmen", "sparen"])]
    _sparen = _df[_df.top == "sparen"].amount.sum()
    _monate = max(_df.monat.nunique(), 1)
    ausgaben_tab = _aus.groupby("top").amount.sum().sort_values().reset_index()
    ausgaben_tab.columns = ["kategorie", "summe"]
    ausgaben_tab["anteil_%"] = (100 * ausgaben_tab.summe / ausgaben_tab.summe.sum()).round(1)
    ausgaben_tab["ø_monat"] = (ausgaben_tab.summe / _monate).round(0)
    _ausgaben_summe = ausgaben_tab.summe.sum()
    _saldo = einnahmen_summe + _ausgaben_summe + _sparen
    ausgaben_table = mo.ui.table(ausgaben_tab.round(2), selection="single", page_size=20)
    mo.vstack(
        [
            mo.md(
                f"## Ausgaben {jahr_select.value} — **{euro(_ausgaben_summe)}**\n"
                f"Sparen: **{euro(_sparen)}** · Saldo: **{euro(_saldo)}** · "
                f"_Zeile anklicken für Details ↓_"
            ),
            ausgaben_table,
        ]
    )
    return ausgaben_tab, ausgaben_table


@app.cell
def _(ausgaben_table, euro, jahres_basis, jahr_select, mo, monatsspalten, pc):
    # Reingehen: Klick auf eine Ausgaben-Zeile -> 2. Ebene pro Monat + Einzelbuchungen
    _sel = ausgaben_table.value
    if _sel is None or len(_sel) == 0:
        drilldown = mo.callout(
            mo.md("**Klick auf eine Zeile der Ausgaben-Tabelle**, um die Kategorie aufzuklappen."),
            kind="neutral",
        )
    else:
        _kat = _sel.iloc[0]["kategorie"]
        _df = jahres_basis(pc, jahr_select.value)
        _sub = _df[_df.top == _kat].copy()
        _sub["ebene2"] = (
            _sub["cat"].fillna("(unkategorisiert)").str.replace(f"{_kat}::", "", regex=False)
        )

        _pivot = _sub.pivot_table(
            index="ebene2", columns="monat", values="amount", aggfunc="sum", fill_value=0.0
        )
        _pivot = monatsspalten(_pivot)
        _pivot["Σ"] = _pivot.sum(axis=1)
        _pivot = _pivot.sort_values("Σ").round(0)
        _pivot = _pivot[["Σ"] + [c for c in _pivot.columns if c != "Σ"]]

        _tx = _sub.sort_values("amount")[
            ["book_date", "account", "ebene2", "party", "purpose", "amount"]
        ].copy()
        _tx["book_date"] = _tx["book_date"].dt.strftime("%d.%m.%Y")
        _tx["purpose"] = _tx["purpose"].fillna("").str.slice(0, 45)
        _tx.columns = ["Datum", "Konto", "Unterkat.", "Party", "Zweck", "Betrag €"]

        drilldown = mo.vstack(
            [
                mo.md(
                    f"### ▸ {_kat} {jahr_select.value} — "
                    f"**{euro(_sub.amount.sum())}** in {len(_sub)} Buchungen"
                ),
                mo.md("**Pro Monat (2. Ebene)**"),
                mo.ui.table(_pivot.reset_index(), page_size=25, selection=None),
                mo.md(f"**Einzelbuchungen** ({len(_tx)})"),
                mo.ui.table(_tx.round(2), page_size=15, selection=None),
            ]
        )
    drilldown
    return


@app.cell
def _(jahres_basis, jahr_select, mo, monatsspalten, pc):
    # Auffächerung pro Monat: Top-Level × Monat, Ø-Spalte, Saldo-Zeile
    _df = jahres_basis(pc, jahr_select.value)
    _matrix = _df.pivot_table(
        index="top", columns="monat", values="amount", aggfunc="sum", fill_value=0.0
    )
    _matrix = monatsspalten(_matrix)
    _matrix["Ø"] = _matrix.mean(axis=1)
    _order = (
        [t for t in ("einnahmen",) if t in _matrix.index]
        + sorted(
            [t for t in _matrix.index if t not in ("einnahmen", "sparen", "(unkategorisiert)")],
            key=lambda t: _matrix.loc[t, "Ø"],
        )
        + [t for t in ("(unkategorisiert)", "sparen") if t in _matrix.index]
    )
    _matrix = _matrix.reindex(_order)
    _matrix.loc["Saldo"] = _matrix.sum()
    mo.vstack(
        [
            mo.md(f"## Monatsmatrix {jahr_select.value}"),
            mo.ui.table(_matrix.round(0).reset_index(), page_size=20),
        ]
    )
    return


@app.cell
def _(FARBE_BLAU, ausgaben_tab, euro, jahr_select, mo, plt):
    # Balkendiagramm: Ausgaben nach Kategorie (eine Farbe = Magnitude, direkte Beschriftung)
    _t = ausgaben_tab.sort_values("summe", ascending=False)  # größte Ausgabe unten
    _fig, _ax = plt.subplots(figsize=(9, 0.42 * len(_t) + 1.2))
    _ax.barh(_t.kategorie, -_t.summe, color=FARBE_BLAU, height=0.62)
    for _y, _v in enumerate(-_t.summe):
        _ax.text(_v, _y, f"  {euro(_v)}", va="center", fontsize=9, color="#52514e")
    _ax.set_title(f"Ausgaben nach Kategorie {jahr_select.value}", loc="left", fontsize=12)
    _ax.set_xlim(0, (-_t.summe).max() * 1.18)
    _ax.tick_params(length=0)
    _ax.xaxis.set_visible(False)
    for _s in ("top", "right", "bottom"):
        _ax.spines[_s].set_visible(False)
    _ax.spines["left"].set_color("#c3c2b7")
    _fig.tight_layout()
    mo.vstack([_fig])
    return


@app.cell
def _(FARBE_AQUA, FARBE_BLAU, jahres_basis, jahr_select, mo, pc, plt):
    # Monatsverlauf: Einnahmen vs. Ausgaben (zwei Serien = zwei Farben + Legende)
    _df = jahres_basis(pc, jahr_select.value)
    _ein = _df[_df.top == "einnahmen"].groupby("monat").amount.sum()
    _aus = -_df[~_df.top.isin(["einnahmen", "sparen"])].groupby("monat").amount.sum()
    _monate = range(1, 13)
    _fig, _ax = plt.subplots(figsize=(9, 3.4))
    _w = 0.38
    _ax.bar(
        [m - _w / 2 for m in _monate],
        [_ein.get(m, 0) for m in _monate],
        width=_w,
        color=FARBE_AQUA,
        label="Einnahmen",
    )
    _ax.bar(
        [m + _w / 2 for m in _monate],
        [_aus.get(m, 0) for m in _monate],
        width=_w,
        color=FARBE_BLAU,
        label="Ausgaben",
    )
    _ax.set_title(f"Einnahmen vs. Ausgaben pro Monat {jahr_select.value}", loc="left", fontsize=12)
    _ax.set_xticks(list(_monate))
    _ax.set_xticklabels(
        ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]
    )
    _ax.tick_params(length=0)
    _ax.grid(axis="y", color="#e1e0d9", linewidth=0.8)
    _ax.set_axisbelow(True)
    for _s in ("top", "right", "left"):
        _ax.spines[_s].set_visible(False)
    _ax.spines["bottom"].set_color("#c3c2b7")
    _ax.legend(frameon=False, loc="upper left")
    _fig.tight_layout()
    mo.vstack([_fig])
    return


if __name__ == "__main__":
    app.run()
