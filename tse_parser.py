"""
funtions to parse tse data
"""
from data_structs import TSEInstrument
from storage import Storage


async def parse_instruments(itd=False) -> dict:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: dict, parsed instrument data
    """
    if itd:
        #TODO: parse intraday instrument data
        raise NotImplementedError
    rows = await Storage().read_tse_csv('tse.instruments')
    instruments = [TSEInstrument(list(row)) for row in rows.values.tolist()]
    return instruments