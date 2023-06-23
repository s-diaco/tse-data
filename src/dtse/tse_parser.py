"""
funtions to parse tse data
"""
import pandas as pd

from .storage import Storage


def parse_instruments(strg: Storage, **kwargs) -> pd.DataFrame:
    """
    Parse instruments list and their data from cached files.

    :return: pd.DataFrame, Instrument data
    """

    instrums = strg.read_tse_csv_blc("tse.instruments")
    if not instrums.empty:
        instrums = instrums.set_index("InsCode")
    return instrums


def parse_splits() -> pd.DataFrame:
    """
    parse shares data (changes in total shares for each symbol)

    :return: pd.DataFrame, parsed shares data
    """
    splits = Storage().read_tse_csv_blc("tse.splits")
    return splits
