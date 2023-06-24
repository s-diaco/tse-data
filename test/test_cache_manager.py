"""
test chache_manager
"""

from pathlib import Path
import time
import numpy as np
import pandas as pd
import pytest
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse import config as cfg

from dtse.storage import Storage


@pytest.fixture(name="sample_instrumnts")
def fixture_resp_data():
    """
    get instruments data
    """

    expected_resp_file = "sample_data/instruments_100_merged.csv"
    inst_col_names = cfg.tse_instrument_info
    merged_inst_col_names = inst_col_names + ["SymbolOriginal"]
    expected_resp = pd.read_csv(
        expected_resp_file, names=merged_inst_col_names, header=0
    )
    yield expected_resp


@pytest.mark.vcr()
async def test_read_prc_csv():
    """
    test read_prc_csv
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    tse_cache_args = {"merge_similar_symbols": True, "cache": False}
    settings = cfg.storage
    settings.update(tse_cache_args)
    cache = TSECache(settings=settings)

    # Creates some file and directories if needed
    Storage()
    if cache.instruments.empty:
        await data_svs.update_instruments(cache)
    instruments = cache.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache.read_prc_csv(selected_syms)
    assert len(cache.stored_prices) <= len(sample_data)


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
