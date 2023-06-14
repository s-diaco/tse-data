"""
tests for the data_services module
"""
import json
import re
import time

import numpy as np
import pandas as pd
import pytest

from dtse import data_services, data_structs
from dtse.cache_manager import TSECache


def test_adjust():
    """
    Test the adjust function.
    """

    cond = 1
    ins_codes = [35796086458096255]

    # parse sample data for closing prices
    sample_closing_prices_path = "sample_data/closing_prices.csv"
    closing_prices = pd.read_csv(sample_closing_prices_path)
    closing_prices = closing_prices.set_index(keys=["InsCode", "DEven"])

    adjusted_closing_prices_path = "sample_data/adjusted_closing_prices.csv"
    expected_res = pd.read_csv(adjusted_closing_prices_path).set_index(
        keys=["InsCode", "DEven"]
    )

    # parse sample data for stock splits
    sample_all_shares_path = "sample_data/all_shares.csv"
    splits = pd.read_csv(sample_all_shares_path)
    splits = splits.set_index(keys=["InsCode", "DEven"])
    splits = splits.drop(labels=["Idn"], axis=1)
    start_time = time.time()
    res = data_services.adjust(cond, closing_prices, splits, ins_codes)
    end_time = time.time()
    total = end_time - start_time
    assert len(np.array(res)) == len(np.array(expected_res))


test_data = [
    ("20220103", "20220302", True),
    ("20220223", "20220223", False),
    ("20220301", "20220304", True),
]


@pytest.mark.parametrize("last_update, last_possible_update, expected", test_data)
def test_should_update(last_update, last_possible_update, expected):
    """
    Test the should_update function.
    """
    res = data_services.should_update(last_update, last_possible_update)
    assert res == expected


@pytest.mark.vcr()
async def test_get_last_possible_deven():
    """
    Test the get_last_possible_deven function.
    """
    res = await data_services.get_last_possible_deven()
    pattern = re.compile(r"^\d{8}$")
    assert pattern.search(res)


@pytest.mark.vcr()
async def test_update_instruments():
    """
    Test the update_instruments function.
    """
    await data_services.update_instruments()
    assert True
