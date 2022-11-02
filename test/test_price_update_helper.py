"""
test price_ipdate_helper
"""
import json

import numpy as np
import pytest
from dtse import price_update_helper as puh


@pytest.fixture
async def resp_data():
    """
    get response data
    """
    test_data_file = 'sample_data/prices_update_helper.json'
    with open(test_data_file, 'r', encoding='utf-8') as f:
        test_data = json.load(f)['_on_result']
        response = test_data['response']
        chunk = np.array(test_data['chunk'])
        on_result_id = test_data['id']
        expected_result = test_data['expected_result']
        yield response, chunk, on_result_id, expected_result


def test_on_result(resp_data):
    """
    test _on_result
    """
    pu_helper = puh.PricesUpdateHelper()
    pu_helper._on_result(resp_data[0], resp_data[1], resp_data[2])


def test_batch():
    """
    test _batch
    """
    test_data_file = 'sample_data/prices_update_helper.json'
    with open(test_data_file, 'r', encoding='utf-8') as f:
        test_data = json.load(f)['_batch']
        chunks = test_data['chunks']
        # expected_result = test_data['expected_result']
    pu_helper = puh.PricesUpdateHelper()
    pu_helper._batch(chunks)
