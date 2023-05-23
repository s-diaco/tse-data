"""
test tse_data.py
"""

import pytest

from dtse import tse_data


@pytest.mark.parametrize(
    "symbols, settings",
    [
        (
            ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "شجم", "نماد غلط"],
            {"adjust_prices": 1},
        ),
        (["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"], {"adjust_prices": 0}),
    ],
    indirect=False,
)
@pytest.mark.asyncio
async def test_get_prices(symbols, settings):
    """
    test get_prices function

    :param symbols: list of symbols
    :param settings: dict of settings
    """
    tse = tse_data.TSE()
    if settings["adjust_prices"]:
        prices = await tse.get_prices(symbols=symbols, adjust_prices=1)
    else:
        prices = await tse.get_prices(symbols=symbols, adjust_prices=0)
    assert prices != "OK"
