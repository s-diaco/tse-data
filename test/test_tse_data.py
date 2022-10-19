"""
test tse_data.py
"""

import pytest

from dtse import tse_data


@pytest.mark.parametrize("symbols, settings", [
    (['همراه', 'ذوب', 'فولاد', 'وبملت', 'شیران', 'نماد غلط'], {'adjust_prices': 1}),
    (['همراه', 'ذوب', 'فولاد', 'شیراز', 'وخارزم'], {'adjust_prices': 0})],
    indirect=False
)
@pytest.mark.asyncio
async def test_get_prices(symbols, settings):
    """
    test get_prices function

    :param symbols: list of symbols
    :param settings: dict of settings
    """
    prices = await tse_data.get_prices(symbols, settings)
    assert prices != 'OK'
