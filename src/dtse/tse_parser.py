"""
funtions to parse tse data
"""
import pandas as pd

from .storage import Storage


def parse_instruments(
    strg: TSEStorage, itd=False, dict_key="InsCode", **kwargs
) -> pd.DataFrame:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: pd.DataFrame, parsed instrument data
    """

    instrums = strg.read_tse_csv_blc("tse.instruments").set_index("InsCode")
    return instrums


def parse_shares() -> pd.DataFrame:
    """
    parse shares data (changes in total shares for each symbol)

    :return: pd.DataFrame, parsed shares data
    """
    splits = Storage().read_tse_csv_blc("tse.shares")
    return splits
