from migration.update_mlb_last10 import _display_venue_name


def test_display_venue_name_shortens_oriole_park() -> None:
    assert _display_venue_name("Oriole Park at Camden Yards") == "Orioles"


def test_display_venue_name_prefers_venue_id() -> None:
    assert _display_venue_name("Any Sponsor Name", 2) == "Orioles"


def test_display_venue_name_uses_id_for_sponsored_venue_name_changes() -> None:
    assert _display_venue_name("UNIQLO Field at Dodger Stadium", 22) == "Dodgers"


def test_display_venue_name_uses_stable_team_name_for_astros_venue() -> None:
    assert _display_venue_name("Daikin Park", 2392) == "Astros"


def test_display_venue_name_uses_stable_team_name_for_giants_venue() -> None:
    assert _display_venue_name("Oracle Park", 2395) == "Giants"


def test_display_venue_name_keeps_unknown_venue() -> None:
    assert _display_venue_name("Fenway Park") == "Fenway Park"
