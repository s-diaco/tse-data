"""
test price_ipdate_helper
"""
import json
import numpy as np
import price_update_helper as puh


def test_on_result():
    """
    test on_result
    """
    test_data_file = 'sample_data/prices_update_helper_on_result.json'
    with open(test_data_file, 'r', encoding='utf-8') as f:
        test_data = json.load(f)['on_result']
        response = test_data['response']
        chunk = np.array(test_data['chunk'])
        on_result_id = test_data['id']
        expected_result = test_data['expected_result']
    pu_helper = puh.PricesUpdateHelper()
    result = pu_helper.on_result(response, chunk, on_result_id)
    assert result == expected_result
