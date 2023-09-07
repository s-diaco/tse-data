"""client for tsetmc api"""

import importlib.metadata

from dtse.core import update_daily_prices

__version__ = importlib.metadata.version(__package__ or __name__)


def get_tse_prices(symbols: list[str], **kwconf):
    """get latest daily prices from database and/or web"""
    return update_daily_prices(symbols=symbols, **kwconf)
