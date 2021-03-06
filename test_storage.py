"""
tests for storage.py
"""
import pytest

import storage


def test_storage():
    """
    test Storage constructor
    """
    instanse = storage.Storage()
    assert instanse.cache_dir != ''


class TestStorage:
    """
    tests for Storage class
    """

    @pytest.mark.parametrize("key, value",
                             [("tse.test1", "67126881188552864,20220223"),
                              ("prices.test2", "20220223")],
                             indirect=False
                             )
    def test_get_item_and_set_item(self, key, value):
        """
        test get_item and set_item
        """
        instanse = storage.Storage()
        instanse.set_item(key, value)
        assert instanse.get_item(key) == value

    @pytest.mark.asyncio
    async def test_set_item_async_and_get_item_async(self):
        """
        test set_item_async and get_item_async
        """
        instanse = storage.Storage()
        await instanse.set_item_async("tse.test_4", "test", tse_zip=True)
        assert await instanse.get_item_async("tse.test_4", tse_zip=True) == 'test'

    @pytest.mark.asyncio
    async def test_get_items(self):
        """
        test get_items
        """
        instanse = storage.Storage()
        res = await instanse.get_items('prices.test2')
        assert len(res) < 2
