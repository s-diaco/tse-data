"""
tests for the data_services module
"""

import re
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from dtse import config as cfg
from dtse import data_services
from dtse.cache_manager import TSECache


@pytest.fixture(name="test_catch")
def fixture_read_prices() -> Generator[TSECache, None, None]:
    """
    providing data for "test_read_prices"
    """

    tse_cache_args = {
        "merge_similar_symbols": True,
        "cache": False,
        "tse_dir": Path("sample_data/prices_not_adj"),
    }
    settings = cfg.storage
    settings.update(tse_cache_args)
    cache = TSECache(settings=settings)
    instrums_file = "sample_data/instruments.csv"
    cache.instruments = pd.read_csv(
        instrums_file, encoding="utf-8", index_col="InsCode"
    )
    yield cache


test_data = [
    ("20220103", "20220302", True),
    ("20220223", "20220223", False),
    ("20220301", "20220304", True),
]


@pytest.mark.parametrize("last_update, last_possible_update, expected", test_data)
def test_should_update(last_update, last_possible_update, expected):
    """
    Test the should_update function.
    """
    res = data_services.should_update(last_update, last_possible_update)
    assert res == expected


@pytest.mark.vcr()
async def test_get_last_possible_deven():
    """
    Test the get_last_possible_deven function.
    """
    res = await data_services.get_last_possible_deven()
    pattern = re.compile(r"^\d{8}$")
    assert pattern.search(res)


@pytest.mark.vcr()
async def test_update_instruments(test_catch):
    """
    Test the update_instruments function.
    """

    cache = test_catch
    await data_services.update_instruments(cache=cache)
    assert True
