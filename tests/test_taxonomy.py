from pathlib import Path

import pytest

from rules import TOP_LEVELS, load_rules, validate_category


def test_top_levels_closed_list() -> None:
    assert "wohnen" in TOP_LEVELS and "sonstiges" in TOP_LEVELS
    assert len(TOP_LEVELS) == 15


def test_validate_category() -> None:
    validate_category("wohnen::moebel::bad")  # ok
    with pytest.raises(ValueError):
        validate_category("moebel::bad")  # altes Top-Level


def test_load_rules_enforces_top_levels(tmp_path: Path) -> None:
    p = tmp_path / "rules.yml"
    p.write_text('- category: handy\n  match: {party: ["congstar"]}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="Top-Level"):
        load_rules(p)


def test_repo_rules_yml_is_valid() -> None:
    load_rules()  # echtes rules.yml muss die geschlossene Liste erfüllen
