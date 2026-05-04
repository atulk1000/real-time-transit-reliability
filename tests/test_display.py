from transit_reliability.display import direction_label


def test_direction_label_maps_wmata_direction_numbers() -> None:
    assert direction_label(1) == "Northbound / Eastbound"
    assert direction_label(2) == "Southbound / Westbound"


def test_direction_label_handles_unknown_values() -> None:
    assert direction_label(None) is None
    assert direction_label(0) is None
    assert direction_label(3) is None

