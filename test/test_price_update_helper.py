"""
test price_update_helper
"""
import json
from pandas import DataFrame

import pytest
from dtse.cache_manager import TSECache
from dtse.data_services import update_instruments

from dtse.price_update_helper import PricesUpdateManager
from dtse.progress_bar import ProgressBar


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


@pytest.mark.vcr()
async def test_start(resp_data):
    """
    test start
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    cache = TSECache(merge_similar_symbols=True, cache=False)
    cache._read_instrums_csv()
    if cache.instruments.empty:
        await update_instruments(cache=cache)
    cache._read_instrums_csv()
    instruments = cache.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache.read_prc_csv(selected_syms)
    pu_helper = PricesUpdateManager(cache)
    update_needed = DataFrame(resp_data, columns=["InsCode", "DEven", "NotInNoMarket"])
    await pu_helper.start(
        update_needed=update_needed,
        settings={"cache": False, "merge_similar_symbols": True},
        progressbar=ProgressBar(),
    )

    # TODO: delete
    """
    ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    """
