"""
test chache_manager
"""

from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from dtse import config as cfg
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


read_prices_params = [
    [
        35796086458096255,
        71483646978964608,
        9211775239375291,
        46348559193224090,
        26787658273107220,
        68635710163497089,
        778253364357513,
    ],
]


@pytest.mark.parametrize("codes", read_prices_params)
def test_read_prices(mocker, codes, test_catch):
    """
    test read_prcices
    """

    daily_prices_dir = "sample_data/"
    daily_prices_list = [
        pd.read_csv(
            f"{daily_prices_dir}prices_not_adj/{str(code)}.csv",
            index_col=["InsCode", "DEven"],
        )
        for code in codes
    ]
    expected_res = pd.concat(daily_prices_list).sort_index()
    cache = test_catch
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
    test_catch: TSECache,
):
    """
    Test adjust function.
    """

    cache = test_catch
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
