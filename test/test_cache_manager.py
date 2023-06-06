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
    cache_manager = TSECache()
    cache_manager.refresh_instrums()
    if cache_manager.instruments.empty:
        await data_svs.update_instruments()
    cache_manager.refresh_instrums()
    instruments = cache_manager.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache_manager.refresh_prices(selected_syms)
    assert len(cache_manager.stored_prices) <= len(sample_data)


def test_refresh_instrums(sample_instrumnts):
    """
    test refresh_instrums
    """

    expected_res = sample_instrumnts
    tse_cache_args = {"merge_similar_symbols": True}
    tse_cache = TSECache(**tse_cache_args)
    test_instrums_path = Path.cwd() / "sample_data"
    refresh_args = {"tse_dir": str(test_instrums_path)}
    tse_cache.refresh_instrums(**refresh_args)
    # make sure res is duplicate of expected_res
    compare_data = pd.concat([tse_cache.merged_instruments, expected_res])
    assert compare_data.drop_duplicates(keep=False).empty
