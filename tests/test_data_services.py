"""
tests for the data_services module
"""

import datetime
import re
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from dtse import config as cfg
from dtse import data_services
from dtse.cache_manager import TSECache


@pytest.fixture(name="test_cache")
def init_cache_inst() -> Generator[TSECache, None, None]:
    """
    providing data for "test_read_prices"
    """

    tse_cache_args = {
        "merge_similar_symbols": True,
        "cache_to_db": False,
        "tse_dir": Path("sample_data"),
    }
    settings = cfg.storage
    settings.update(tse_cache_args)
    cache = TSECache(settings=settings)
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
async def test_get_last_possible_deven(test_cache):
    """
    Test the get_last_possible_deven function.
    """
    res = await data_services.get_last_possible_deven(test_cache.last_possible_deven)
    pattern = re.compile(r"^\d{8}$")
    assert pattern.search(res)


last_poss_date = ["20220809"]


@pytest.mark.parametrize("last_p_d", last_poss_date)
@pytest.mark.vcr(record_mode="new_episodes")
async def test_update_instruments(test_cache, mocker, last_p_d):
    """
    Test the update_instruments function.
    """

    cache = test_cache
    assert cache.instruments is None

    mock_last_p_date = mocker.patch("dtse.data_services.get_last_possible_deven")
    mock_last_p_date.return_value = last_p_d
    mock_date = mocker.patch("dtse.data_services.datetime")
    mock_date.now.return_value = datetime.datetime(2023, 8, 21)
    await data_services.update_instruments(cache=cache)
    exp_res_ins = pd.read_csv("sample_data/instruments.csv", index_col="InsCode")
    pd.testing.assert_frame_equal(cache.instruments, exp_res_ins)
    exp_res_spl = pd.read_csv("sample_data/shares.csv", index_col=["InsCode", "DEven"])
    pd.testing.assert_frame_equal(cache.splits, exp_res_spl)
