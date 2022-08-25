"""
test tse_data.py
"""

import pytest

import tse_data


@pytest.mark.parametrize("symbols, settings", [
    (['همراه', 'ذوب', 'فولاد', 'وبملت', 'شیران'], {'adjust_prices': 1}),
    (['همراه', 'ذوب', 'فولاد', 'شیراز', 'وبملت'], {'adjust_prices': 0})],
    indirect=False
)
@pytest.mark.asyncio
async def test_get_prices(symbols, settings):
    """
    test get_prices function
    """
    resp = await tse_data.get_prices(symbols=symbols, _settings=settings)
    assert resp != 'OK'
