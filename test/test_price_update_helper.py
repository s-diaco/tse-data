"""
test price_ipdate_helper
"""
import json

import numpy as np
import pytest
from dtse import price_update_helper as puh


@pytest.fixture
def resp_data():
    """
    get response data
    """
    test_data_file = "sample_data/prices_update_helper.json"
    with open(test_data_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)["_on_result"]
        response = test_data["response"]
        chunk = np.array(test_data["chunk"])
        on_result_id = test_data["id"]
        expected_result = test_data["expected_result"]
        yield response, chunk, on_result_id, expected_result


async def test_start(resp_data):
    """
    test start
    """
    pu_helper = puh.PricesUpdateHelper()
    await pu_helper.start(
        update_needed=["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    )
