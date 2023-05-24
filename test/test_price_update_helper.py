"""
test price_ipdate_helper
"""
import json

import pytest
from dtse.cache_manager import TSECachedData

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

    cache_manager = TSECachedData()
    pu_helper = PricesUpdateHelper(cache_manager)
    update_needed = resp_data
    await pu_helper.start(
        update_needed=update_needed,
        settings={"shoud_cache": True},
        progressbar=ProgressBar(),
    )

    # TODO: delete
    """
    ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    """
