"""
funtions to parse tse data
"""
import pandas as pd

from .storage import Storage


def parse_instruments(itd=False, dict_key="InsCode", **kwargs) -> pd.DataFrame:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: pd.DataFrame, parsed instrument data
    """
    if itd:
        # TODO: parse intraday instrument data
        raise NotImplementedError
    strg = Storage(**kwargs)
    instrums = strg.read_tse_csv_blc("tse.instruments")
    return instrums


def parse_shares() -> pd.DataFrame:
    """
    parse shares data

    :return: pd.DataFrame, parsed shares data
    """
    rows = Storage().read_tse_csv_blc("tse.shares")
    if len(rows.index):
        # TODO: delete
        """
        shares = [TSEShare(row) for row in rows.values.tolist()]
        shares_dict = dict(zip(rows['Idn'], shares))
        return shares_dict
        """
        return rows
    return pd.DataFrame()
