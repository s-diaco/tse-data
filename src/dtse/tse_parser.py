"""
funtions to parse tse data
"""
import numpy as np
import pandas as pd
from .data_structs import TSEInstrument, TSEShare
from .storage import Storage


async def parse_instruments(itd=False, dict_key='InsCode') -> pd.DataFrame:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: dict, parsed instrument data
    """
    if itd:
        #TODO: parse intraday instrument data
        raise NotImplementedError
    rows = await Storage().read_tse_csv('tse.instruments')
    rows = rows.fillna(np.nan).replace([np.nan], [None])
    # TODO: delete this if statement
    """
    if len(rows.index):
        instruments = [TSEInstrument(row) for row in rows.values.tolist()]
        instruments_dict = dict(zip(rows[dict_key], instruments))
        return instruments_dict
    """
    return rows

async def parse_shares() -> dict:
    """
    parse shares data

    :return: dict, parsed shares data
    """
    rows = await Storage().read_tse_csv('tse.shares')
    rows = rows.fillna(np.nan).replace([np.nan], [None])
    if len(rows.index):
        shares = [TSEShare(row) for row in rows.values.tolist()]
        shares_dict = dict(zip(rows['Idn'], shares))
        return shares_dict
    return {}
    