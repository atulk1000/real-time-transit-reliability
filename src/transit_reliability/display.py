from __future__ import annotations


def direction_label(direction_num: int | None) -> str | None:
    if direction_num == 1:
        return "Northbound / Eastbound"
    if direction_num == 2:
        return "Southbound / Westbound"
    return None
