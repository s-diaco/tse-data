"""
funtions to parse tse data
"""
import pandas as pd

from . import config as cfg
from .storage import Storage


async def parse_instruments(itd=False, dict_key="InsCode") -> pd.DataFrame:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: pd.DataFrame, parsed instrument data
    """
    if itd:
        # TODO: parse intraday instrument data
        raise NotImplementedError
    csv_rows = await Storage().read_tse_csv("tse.instruments")
    return csv_rows


async def parse_shares() -> pd.DataFrame:
    """
    parse shares data

    :return: pd.DataFrame, parsed shares data
    """
    rows = await Storage().read_tse_csv("tse.shares")
    if len(rows.index):
        # TODO: delete
        """
        shares = [TSEShare(row) for row in rows.values.tolist()]
        shares_dict = dict(zip(rows['Idn'], shares))
        return shares_dict
        """
        return rows
    return pd.DataFrame()
