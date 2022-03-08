"""
tests for the data_services module
"""
import json
import re

import numpy as np
import pytest

import data_services
import data_structs


def test_adjust():
    """
    Test the adjust function.
    """
    cond = 1
    ins_codes = set(['68635710163497089', '26787658273107220'])

    # parse sample data for closing prices
    sample_closing_prices_path = 'sample_data/closing_prices.json'
    with open(sample_closing_prices_path, 'r', encoding='utf-8') as f_cp:
        closing_prices_json = json.loads(f_cp.read())
    closing_prices = [data_structs.TSEClosingPrice(**cp_dict)
                      for cp_dict in closing_prices_json]

    # parse sample data for shares
    sample_all_shares_path = 'sample_data/all_shares.json'
    with open(sample_all_shares_path, 'r', encoding='utf-8') as f_as:
        all_shares_json = json.loads(f_as.read())
    all_shares = [data_structs.TSEShare(**share_dict)
                  for share_dict in all_shares_json]

    res = data_services.adjust(cond, closing_prices, all_shares, ins_codes)
    assert np.array(res).shape != (2, 2)


@pytest.mark.parametrize("last_update, last_possible_update, expected", [
                        ("20220103", "20220302", True),
                        ("20220223", "20220223", False),
                        ("20220302", "20220304", False)],
    indirect=False
)
def test_should_update(last_update, last_possible_update, expected):
    """
    Test the should_update function.
    """
    res = data_services.should_update(last_update, last_possible_update)
    assert res == expected


@pytest.mark.asyncio
async def test_get_last_possible_deven():
    """
    Test the get_last_possible_deven function.
    """
    res = await data_services.get_last_possible_deven()
    pattern = re.compile(r'^\d{8}$')
    assert pattern.search(res)


@pytest.mark.asyncio
async def test_update_instruments():
    """
    Test the update_instruments function.
    """
    await data_services.update_instruments()
