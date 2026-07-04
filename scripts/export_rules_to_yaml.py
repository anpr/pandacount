"""Einmal-Skript: exportiert panda.CATEGORY_RULES_MAP nach rules.yml.

Reihenfolge bleibt erhalten (Dict-Insertion-Order = Anwendungsreihenfolge).
Tuple-Einträge (account, pattern) werden zu eigenen account-gescopten Regeln.
"""

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from panda import CATEGORY_RULES_MAP  # noqa: E402


def to_rule_dicts(rules_map: dict[str, dict]) -> list[dict]:
    out: list[dict] = []
    for category, attr_map in rules_map.items():
        plain: dict[str, list[str]] = {}
        scoped: dict[str, dict[str, list[str]]] = {}
        for attr, subs in attr_map.items():
            for sub in subs:
                if isinstance(sub, tuple):
                    account, pattern = sub
                    scoped.setdefault(account, {}).setdefault(attr, []).append(pattern)
                else:
                    plain.setdefault(attr, []).append(sub)
        if plain:
            out.append({"category": category, "match": plain})
        for account, match in scoped.items():
            out.append({"category": category, "match": match, "account": account})
    return out


if __name__ == "__main__":
    target = Path(__file__).parent.parent / "rules.yml"
    target.write_text(
        yaml.dump(
            to_rule_dicts(CATEGORY_RULES_MAP),
            allow_unicode=True,
            sort_keys=False,
            width=100,
        ),
        encoding="utf-8",
    )
    print(f"{target} geschrieben")
