"""
test chache_manager
"""

import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dtse import config as cfg
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse.storage import Storage


@pytest.fixture(name="sample_instrumnts")
def fixture_resp_data():
    """
    get instruments data
    """

    # TODO: fixture never uesd
    expected_resp_file = "sample_data/instruments_100_merged.csv"
    inst_col_names = cfg.tse_instrument_info
    merged_inst_col_names = inst_col_names + ["SymbolOriginal"]
    expected_resp = pd.read_csv(
        expected_resp_file, names=merged_inst_col_names, header=0
    )
    yield expected_resp


@pytest.fixture(name="read_prices_data")
def fixture_read_prices():
    """
    providing data for "test_read_prices"
    """
    # sample symbols = ["همراه", "ذوب", "فولاد", "وبملت", "شیران"]
    tse_cache_args = {
        "merge_similar_symbols": True,
        "cache": False,
        "tse_dir": Path("sample_data/prices"),
    }
    settings = cfg.storage
    settings.update(tse_cache_args)
    cache = TSECache(settings=settings)
    instrums_file = "sample_data/instruments_all.csv"
    cache.instruments = pd.read_csv(
        instrums_file, encoding="utf-8", index_col="InsCode"
    )
    sample_prices_file = "sample_data/sample_cache.prices.csv"
    sample_prices = pd.read_csv(
        sample_prices_file, encoding="utf-8", index_col=["InsCode", "DEven"]
    )
    yield cache, sample_prices


def test_read_prices(read_prices_data):
    """
    test read_prcices
    """

    cache, expected_res = read_prices_data
    selected_syms_file = "sample_data/sample_selected_syms.csv"
    selected_syms = pd.read_csv(
        selected_syms_file, encoding="utf-8", index_col="InsCode"
    )
    cache.read_prices(selected_syms)
    pd.testing.assert_frame_equal(cache.prices, expected_res)


adjust_params = [
    (0, [35796086458096255]),  # شیران
    (1, [68635710163497089, 26787658273107220]),  # همراه
    (2, [35796086458096255]),
    (3, [68635710163497089, 26787658273107220]),
]


@pytest.mark.parametrize("cond, codes", adjust_params)
def test_adjust(
    cond: int,
    codes: list[int],
    read_prices_data: tuple[TSECache, pd.DataFrame],
):
    """
    Test adjust function.
    """

    cache, closing_prices = read_prices_data
    if cond == 0:
        adjusted_closing_prices_path = "sample_data/sample_cache.prices.csv"
    if cond == 1:
        adjusted_closing_prices_path = (
            "sample_data/prices_adjusted_cond_1/" + str(codes[0]) + ".csv"
        )
    if cond == 2:
        adjusted_closing_prices_path = (
            "sample_data/prices_adjusted_cond_2/" + str(codes[0]) + ".csv"
        )
    if cond == 3:
        adjusted_closing_prices_path = (
            "sample_data/prices_adjusted_cond_1/" + str(codes[0]) + ".csv"
        )
    expected_res = pd.read_csv(
        adjusted_closing_prices_path, index_col=["InsCode", "DEven"]
    )

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    assert cache.prices is None
    cache.add_to_prices([closing_prices])
    cache.splits = splits
    res = cache.adjust(cond, codes)
    assert len(np.array(res)) == len(np.array(expected_res))


refresh_params = [
    [35796086458096255],  # شیران
    [68635710163497089, 26787658273107220],  # همراه
    [9211775239375291,    71483646978964608],  # ذوب
]


@pytest.mark.parametrize("codes", refresh_params)
def test_refresh_prices_merged(
    codes: list[int],
    read_prices_data: tuple[TSECache, pd.DataFrame],
):
    """
    Test refresh_prices_merged function.
    """

    adjusted_closing_prices_path = "sample_data/sample_cache.prices.csv"
    cache, closing_prices = read_prices_data
    expected_res = pd.read_csv(
        adjusted_closing_prices_path, index_col=["InsCode", "DEven"]
    )

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    selected_syms_file = "sample_data/sample_selected_syms.csv"
    selected_syms = pd.read_csv(
        selected_syms_file, encoding="utf-8", index_col="InsCode"
    )
    assert cache.prices is None
    cache.add_to_prices([closing_prices])
    cache.splits = splits
    res = cache.refresh_prices_merged(selected_syms)
    assert len(np.array(res)) == len(np.array(expected_res))
