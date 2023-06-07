"""
test tse_data.py
"""

import pytest

from dtse.tse_data import TSE


@pytest.mark.parametrize(
    "symbols, settings",
    [
        (["همراه", "فولاد", "شجم", "نماد غلط"], {"adjust_prices": 1}),
        (["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"], {"adjust_prices": 0}),
        (["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"], {"adjust_prices": 2}),
    ],
    indirect=False,
)
async def test_get_prices(symbols, settings):
    """
    test get_prices function

    :param symbols: list of symbols
    :param settings: dict of settings
    """
    tse = TSE()
    if settings["adjust_prices"]:
        prices = await tse.get_prices(symbols=symbols, adjust_prices=1)
    else:
        prices = await tse.get_prices(symbols=symbols, adjust_prices=0)
    assert prices != "OK"
