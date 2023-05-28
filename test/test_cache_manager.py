"""
test chache_manager
"""

import pandas as pd
import pytest
from dtse import data_services as data_svs
from dtse.cache_manager import TSECache
from dtse import config as cfg


@pytest.fixture(name="sample_instrumnts")
def fixture_resp_data():
    """
    get instruments data
    """

    test_insts_file = "sample_data/instruments_100.csv"
    expected_resp_file = "sample_data/instruments_100_merged.csv"
    converters = {"InsCode": int}
    inst_col_names = cfg.tse_instrument_info
    test_insts = pd.read_csv(
        test_insts_file, names=inst_col_names, converters=converters
    )
    merged_inst_col_names = inst_col_names + ["SymbolOriginal"]
    expected_resp = pd.read_csv(
        expected_resp_file, names=merged_inst_col_names, converters=converters
    )
    yield (test_insts, expected_resp)


async def test_refresh_prices():
    """
    test refresh_prices
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    cache_manager = TSECache()
    cache_manager.refresh_instrums()
    if cache_manager.instruments.empty:
        await data_svs.update_instruments()
    cache_manager.refresh_instrums()
    instruments = cache_manager.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache_manager = TSECache()
    cache_manager.refresh_prices(selected_syms)
    assert len(cache_manager.stored_prices) <= len(sample_data)


def test_refresh_instrums(sample_instrumnts):
    """
    test refresh_instrums
    """

    sample_data, expected_result = sample_instrumnts
    tse_cache_args = {"merge_similar_symbols": True}
    tse_cache = TSECache(**tse_cache_args)
    tse_cache.instruments = sample_data
    res = tse_cache._merge_similar_syms()
    assert ~tse_cache.instruments.empty
