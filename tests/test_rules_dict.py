from panda import CATEGORY_RULES_MAP


def test_rules_map_is_module_level() -> None:
    assert isinstance(CATEGORY_RULES_MAP, dict)
    assert "wohnen::strom" in CATEGORY_RULES_MAP
    assert "gesa::amazon" in CATEGORY_RULES_MAP
    # Tuple-Einträge (account-gescoped) bleiben erhalten
    assert any(
        isinstance(sub, tuple)
        for sub in CATEGORY_RULES_MAP["gesa::amazon"]["party"]
    )
