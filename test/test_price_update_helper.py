"""
test price_ipdate_helper
"""
import json
from pandas import DataFrame

import pytest
from dtse.cache_manager import TSECache
from dtse.data_services import update_instruments

from dtse.price_update_helper import PricesUpdateHelper
from dtse.progress_bar import ProgressBar


@pytest.fixture(name="resp_data")
def fixture_resp_data():
    """
    get response data
    """
    test_data_file = "sample_data/prices_update_helper.json"
    with open(test_data_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)["start"]
        update_needed = test_data["update_needed"]
        yield update_needed


async def test_start(resp_data):
    """
    test start
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    cache_manager = TSECache()
    cache_manager.refresh_instrums()
    if cache_manager.instruments.empty:
        await update_instruments()
    cache_manager.refresh_instrums()
    instruments = cache_manager.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache_manager.refresh_prices(selected_syms)
    pu_helper = PricesUpdateHelper(cache_manager)
    update_needed = DataFrame(resp_data, columns=["InsCode", "DEven", "NotInNoMarket"])
    await pu_helper.start(
        update_needed=update_needed,
        settings={"should_cache": True, "merge_similar_symbols": True},
        progressbar=ProgressBar(),
    )

    # TODO: delete
    """
    ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    """
