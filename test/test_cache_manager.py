"""
test chache_manager
"""

from pathlib import Path
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

    expected_resp_file = "sample_data/instruments_100_merged.csv"
    inst_col_names = cfg.tse_instrument_info
    merged_inst_col_names = inst_col_names + ["SymbolOriginal"]
    expected_resp = pd.read_csv(
        expected_resp_file, names=merged_inst_col_names, header=0
    )
    yield expected_resp


async def test_refresh_prices():
    """
    test refresh_prices
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    tse_cache_args = {"merge_similar_symbols": True, "cache": False}
    cache_manager = TSECache(**tse_cache_args)
    if cache_manager.instruments.empty:
        await data_svs.update_instruments()
    instruments = cache_manager.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache_manager.refresh_prices(selected_syms)
    assert len(cache_manager.stored_prices) <= len(sample_data)


def test_get_symbol_names():
    """
    test the get_symbol_name method
    """

    ins_codes = ["778253364357513", "9211775239375291", "26787658273107220"]
    cache = TSECache()
    symbol_names = cache.get_symbol_names(ins_codes=ins_codes)
    expected_result = ["وبملت", "ذوب", "همراه"]
    assert list(symbol_names.values()) == expected_result
