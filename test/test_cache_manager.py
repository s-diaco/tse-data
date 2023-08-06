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
        "tse_dir": Path("sample_data/prices_not_adj"),
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
    cache, closing_prices = read_prices_data
    expected_res = pd.read_csv(merged_prices_file, index_col=["Symbol", "DEven"])

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
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


adjust_params = [
    (0, [35796086458096255]),  # شیران
    (1, [35796086458096255]),
    (2, [35796086458096255]),
    (3, [35796086458096255]),
    (1, [71483646978964608, 9211775239375291]),  # ذوب
    (2, [71483646978964608, 9211775239375291]),
    (3, [71483646978964608, 9211775239375291]),
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

    cache, _ = read_prices_data
    adj_daily_prices_dir = "sample_data/"
    if cond == 0:
        adj_daily_prices_dir += "prices_not_adj/"
    if cond == 1:
        adj_daily_prices_dir += "prices_adjusted_cond_1/"
    if cond == 2:  # افزایش سرمایه با احتساب آورده
        adj_daily_prices_dir += "prices_adjusted_cond_2/"
    if cond == 3:
        adj_daily_prices_dir += "prices_adjusted_cond_1/"
    res_list = [
        pd.read_csv(
            f"{adj_daily_prices_dir}{str(code)}.csv", index_col=["InsCode", "DEven"]
        )
        for code in codes
    ]
    not_adj_prices_list = [
        pd.read_csv(
            f"sample_data/prices_not_adj/{str(code)}.csv",
            index_col=["InsCode", "DEven"],
        )
        for code in codes
    ]
    expected_res = pd.concat(res_list)
    expected_res = expected_res.sort_index(level=1)

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    assert cache.prices is None
    cache.add_to_prices(not_adj_prices_list)
    cache.splits = splits
    res = cache.adjust(cond, codes)
    if cond:
        res = res.drop(["PClosing"], axis=1)
        res = res[["AdjPClosing"]].rename({"AdjPClosing": "PClosing"}, axis=1)
    if res is not None:
        pd.testing.assert_frame_equal(
            left=res[["PClosing"]],
            right=expected_res[["PClosing"]],
            atol=1,
        )
