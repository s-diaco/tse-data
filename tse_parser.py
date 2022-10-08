"""
funtions to parse tse data
"""
import numpy as np
from data_structs import TSEInstrument
from storage import Storage


async def parse_instruments(itd=False, dict_key='InsCode') -> dict:
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
    if len(rows.index):
        instruments = [TSEInstrument(row) for row in rows.values.tolist()]
        instruments_dict = dict(zip(rows[dict_key], instruments))
        return instruments_dict
    raise Exception('No instrument data found')
    