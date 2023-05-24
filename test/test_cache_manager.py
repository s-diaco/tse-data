"""
test chache_manager
"""

from dtse import data_services as data_svs
from dtse.cache_manager import TSECachedData


async def test_update_stored_prices():
    """
    test update_stored_prices
    """

    sample_data = ["همراه", "ذوب", "فولاد", "وبملت", "شیران", "نماد غلط"]
    cache_manager = TSECachedData()
    cache_manager.upd_cached_instrums()
    if cache_manager.instruments.empty:
        await data_svs.update_instruments()
    cache_manager.upd_cached_instrums()
    instruments = cache_manager.instruments
    selected_syms = instruments[instruments["Symbol"].isin(sample_data)]
    cache_manager = TSECachedData()
    cache_manager.upd_cached_prices(selected_syms)
    assert len(cache_manager.stored_prices) <= len(sample_data)
