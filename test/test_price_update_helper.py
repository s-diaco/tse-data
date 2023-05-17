"""
test price_ipdate_helper
"""
import json

import pytest

from dtse import price_update_helper as puh


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
    pu_helper = puh.PricesUpdateHelper()
    update_needed = resp_data
    await pu_helper.start(update_needed=update_needed, should_cache=True)

    # TODO: delete
    """
    ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    """
