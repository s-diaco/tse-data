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
    # sample symbols = ["همراه", "ذوب", "فولاد", "وبملت", "شیران"]
    tse_cache_args = {"merge_similar_symbols": True, "cache": False}
    settings = cfg.storage
    settings.update(tse_cache_args)
    cache = TSECache(settings=settings)
    instrums_file = "sample_data/instruments_all.csv"
    cache.instruments = pd.read_csv(
        instrums_file, encoding="utf-8", index_col="InsCode"
    )
    selected_syms_file = "sample_data/sample_selected_syms.csv"
    selected_syms = pd.read_csv(
        selected_syms_file, encoding="utf-8", index_col="InsCode"
    )
    yield cache, selected_syms


def mock_read_prc_csv(f_names: list[str]) -> pd.DataFrame:
    """
    mock _read_prc_csv for tests.

    :f_names: list[str], list of file names to read from.

    :return: pd.DataFrame
    """

    csv_dir = "sample_data/"
    prices_list = [
        pd.read_csv(
            csv_dir + f"{name}.csv",
            encoding="utf-8",
            index_col=["InsCode", "DEven"],
        )
        for name in f_names
        if Path(csv_dir + f"{name}.csv").is_file()
    ]
    prices_list = [prcs for prcs in prices_list if not prcs.empty]
    if prices_list:
        res = pd.concat(prices_list)
    else:
        res = pd.DataFrame()
    return res


@pytest.mark.vcr()
def test_read_prices(mocker, read_prices_data):
    """
    test read_prcices
    """

    cache, selected_syms = read_prices_data
    mock_read_csv = mocker.patch(
        "dtse.cache_manager:TSECache._read_prc_csv",
        side_effect=mock_read_prc_csv,
    )
    mock_read_csv.side_effect = mock_read_prc_csv
    cache.read_prices(selected_syms)
    assert len(cache.prices) <= len(selected_syms)


def test_get_symbol_names():
    """
    test the get_symbol_name method
    """

    ins_codes = ["778253364357513", "9211775239375291", "26787658273107220"]
    cache = TSECache()
    symbol_names = cache.get_symbol_names(ins_codes=ins_codes)
    expected_result = ["وبملت", "ذوب", "همراه"]
    assert list(symbol_names.values()) == expected_result


def test_adjust():
    """
    Test the adjust function.
    """

    cond = 1
    ins_codes = [35796086458096255]

    # parse sample data for closing prices
    sample_closing_prices_path = "sample_data/closing_prices.csv"
    closing_prices = pd.read_csv(
        sample_closing_prices_path, index_col=["InsCode", "DEven"]
    )

    adjusted_closing_prices_path = "sample_data/adjusted_closing_prices.csv"
    expected_res = pd.read_csv(
        adjusted_closing_prices_path, index_col=["InsCode", "DEven"]
    )

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
    splits = pd.read_csv(sample_all_shares_path, index_col=["InsCode", "DEven"])
    cache = TSECache()
    start_time = time.time()
    res = cache.adjust(cond, closing_prices, ins_codes)
    end_time = time.time()
    total = end_time - start_time
    assert len(np.array(res)) == len(np.array(expected_res))
