"""
test tse_data.py
"""

import logging
import pytest

from dtse.tse_data import TSE


@pytest.mark.parametrize(
    "symbols, settings, exp_res",
    [
        (
            ["همراه", "فولاد", "شجم", "نماد غلط"],
            {"adjust_prices": 1},
            "symbols not found: نماد غلط",
        ),
        (
            ["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"],
            {"adjust_prices": 0},
            None,
        ),
        (
            ["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"],
            {"adjust_prices": 2},
            None,
        ),
    ],
)
@pytest.mark.vcr(record_mode="new_episodes")
async def test_get_prices(symbols, settings, exp_res, caplog):
    """
    test get_prices function
    """
    caplog.set_level(logging.DEBUG)
    tse = TSE()
    if settings["adjust_prices"]:
        prices = await tse.get_prices(
            symbols=symbols, adjust_prices=1, cache_to_db=False
        )
    else:
        prices = await tse.get_prices(
            symbols=symbols, adjust_prices=0, cache_to_db=False
        )
    assert prices != "OK"
    if exp_res:
        assert exp_res in [x.message for x in caplog.records if x.name in ["dtse"]]
