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


@pytest.fixture(name="read_prices_data")
def fixture_read_prices():
    """
    providing data for "test_read_prices"
    """
    # sample symbols = ["همراه", "ذوب", "فولاد", "وبملت", "شیران"]
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


refresh_params = [
    ([35796086458096255], "شیران"),
    ([68635710163497089, 26787658273107220], "همراه"),
    ([9211775239375291, 71483646978964608], "ذوب"),
]


@pytest.mark.parametrize("codes, symbol", refresh_params)
def test_refresh_prices_merged(
    codes: list[int],
    symbol: str,
    read_prices_data: tuple[TSECache, pd.DataFrame],
):
    """
    Test refresh_prices_merged function.
    """

    prices_files = [f"sample_data/prices/{prc_file}.csv" for prc_file in codes]
    prcs_list = [
        pd.read_csv(prc_file, index_col=["InsCode", "DEven"])
        for prc_file in prices_files
    ]
    merged_prices_file = f"sample_data/prices_merged/{symbol}.csv"
    cache = read_prices_data
    expected_res = pd.read_csv(merged_prices_file, index_col=["Symbol", "DEven"])

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    selected_syms_file = "sample_data/sample_selected_syms.csv"
    selected_syms = pd.read_csv(
        selected_syms_file, encoding="utf-8", index_col="InsCode"
    )
    assert cache.prices is None
    cache.add_to_prices(prcs_list)
    cache.splits = splits
    cache.refresh_prices_merged(selected_syms)
    assert cache.prices_merged is not None
    if cache.prices_merged is not None:
        pd.testing.assert_frame_equal(cache.prices_merged, expected_res)


def test_read_prices(mocker, read_prices_data):
    """
    test read_prcices
    """

    cache, expected_res = read_prices_data
    mock_sql = mocker.patch("dtse.cache_manager.pd.read_sql")
    mock_sql.return_value = expected_res
    selected_syms_file = "sample_data/sample_selected_syms.csv"
    selected_syms = pd.read_csv(
        selected_syms_file, encoding="utf-8", index_col="InsCode"
    )
    cache.read_prices(selected_syms)
    pd.testing.assert_frame_equal(cache.prices, expected_res)


conds = [0, 1, 2]
adjust_params = [
    ([35796086458096255], "شیران"),
    ([71483646978964608, 9211775239375291], "ذوب"),
    ([47377315952751604], "بسویچ"),
    ([46348559193224090], "فولاد"),
    ([26787658273107220, 68635710163497089], "همراه"),
    ([778253364357513], "وبملت"),
    ([54676885047867737], "وتوشه"),
]


@pytest.mark.parametrize("cond", conds)
@pytest.mark.parametrize("codes, res_file", adjust_params)
def test_adjust(
    cond: int,
    codes: list[int],
    res_file: str,
    read_prices_data: tuple[TSECache, pd.DataFrame],
):
    """
    Test adjust function.
    """

    cache = read_prices_data
    adj_daily_prices_dir = "sample_data/"
    not_adj_prices_list = [
        pd.read_csv(
            f"{adj_daily_prices_dir}prices_not_adj/{str(code)}.csv",
            index_col=["InsCode", "DEven"],
        )
        for code in codes
    ]
    expected_res = pd.read_csv(
        f"{adj_daily_prices_dir}prices_adjusted_cond_{cond}/{res_file}.csv",
        index_col="DEven",
    )
    expected_res = expected_res.sort_index()
    expected_res = expected_res[expected_res.index > 20200526]
    expected_res = expected_res[expected_res["QTotTran5J"] != 0]

    # parse sample data for stock splits
    sample_all_shares_path = f"{adj_daily_prices_dir}shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    assert cache.prices is None
    cache.add_to_prices(not_adj_prices_list)
    cache.splits = splits
    res = cache.adjust(cond, codes)
    res = res[res["QTotTran5J"] != 0]
    res = res.reset_index().set_index("DEven").sort_index()
    trim_len = min(len(res), len(expected_res))
    if cond:
        res = res.drop(["PClosing"], axis=1)
        res = res[["AdjPClosing"]].rename({"AdjPClosing": "PClosing"}, axis=1)
    if res is not None:
        pd.testing.assert_frame_equal(
            left=res[["PClosing"]].iloc[-trim_len:],
            right=expected_res[["PClosing"]].iloc[-trim_len:],
            atol=10,
            check_dtype=False,
        )
