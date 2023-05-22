"""
test chache_manager
"""

from dtse.cache_manager import TSECachedData
from dtse.tse_data import get_valid_syms


async def test_update_stored_prices():
    """
    test update_stored_prices
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    sample_data_df = await get_valid_syms(sample_data)
    cache_manager = TSECachedData(sample_data_df)
    cache_manager.update_stored_prices()
    assert len(cache_manager.stored_prices) <= len(sample_data)
