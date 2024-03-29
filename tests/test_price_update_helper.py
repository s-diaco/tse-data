"""
test price_update_helper
"""
import json
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from dtse import config as cfg
from dtse.cache_manager import TSECache
from dtse.data_services import update_instruments
from dtse.price_updater import PriceUpdater


@pytest.fixture(name="test_catch")
def fixture_read_prices() -> Generator[TSECache, None, None]:
    """
    providing data for "test_read_prices"
    """

    tse_cache_args = {
        "merge_similar_symbols": True,
        "cache_to_db": False,
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


@pytest.fixture(name="resp_data")
def fixture_resp_data():
    """
    get response data
    """
    test_data_file = "sample_data/prices_update_helper.json"
    with open(test_data_file, "r", encoding="utf-8") as file:
        test_data = json.load(file)["start"]
        update_needed = test_data["update_needed"]
        yield update_needed


@pytest.mark.vcr(record_mode="new_episodes")
async def test_start(resp_data, test_catch):
    """
    test start()
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    cache = test_catch
    if cache.instruments.empty:
        await update_instruments(cache=cache)
    instruments = cache.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache.read_prices(selected_syms)
    pu_helper = PriceUpdater(cache)
    outdated_insts = pd.DataFrame(
        resp_data, columns=["InsCode", "DEven", "NotInNoMarket"]
    ).set_index("InsCode")
    ret_val = await pu_helper.update_prices(outdated_insts=outdated_insts)
    assert set(["succs", "fails"]) == set(ret_val)
