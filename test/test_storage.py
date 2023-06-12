"""
tests for storage.py
"""
import pytest

from dtse import storage


test_data = [("tse.test1", "67126881188552864,20220223"), ("prices.test2", "20220223")]


@pytest.mark.parametrize("key, value", test_data)
def test_get_item_and_set_item(key, value):
    """
    test get_item and set_item
    """

    instanse = storage.Storage()
    instanse.set_item(key, value)
    assert instanse.get_item(key) == value


async def test_get_items():
    """
    test get_items
    """

    instanse = storage.Storage()
    res = instanse.get_items(["prices.test2"])
    assert len(res) < 2
