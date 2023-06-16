"""
tests for the data_services module
"""

import re

import numpy as np
import pandas as pd
import pytest

from dtse.cache_manager import TSECache

from dtse import data_services

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

    cache = TSECache()
    await data_services.update_instruments(cache=cache)
    assert True
