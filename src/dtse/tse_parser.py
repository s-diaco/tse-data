"""
funtions to parse tse data
"""
import pandas as pd

from . import config as cfg
from .storage import Storage


async def parse_instruments(itd=False, dict_key='InsCode') -> pd.DataFrame:
    """
    parse instrument data

    :param itd: bool, if True, parse instruments in intraday data api

    :return: pd.DataFrame, parsed instrument data
    """
    if itd:
        # TODO: parse intraday instrument data
        raise NotImplementedError
    csv_rows = await Storage().read_tse_csv('tse.instruments')
    if len(csv_rows.index):
        rows = _procc_similar_syms(csv_rows)
        return rows
    return pd.DataFrame()


async def parse_shares() -> pd.DataFrame:
    """
    parse shares data

    :return: pd.DataFrame, parsed shares data
    """
    rows = await Storage().read_tse_csv('tse.shares')
    if len(rows.index):
        # TODO: delete
        """
        shares = [TSEShare(row) for row in rows.values.tolist()]
        shares_dict = dict(zip(rows['Idn'], shares))
        return shares_dict
        """
        return rows
    return pd.DataFrame()


def _procc_similar_syms(instrums_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process similar symbols an add "SymbolOriginal" column to DataFrame

    :param instrums_df: pd.DataFrame, instruments dataframe

    :return: pd.DataFrame, processed dataframe
    """
    sym_groups = [x for x in instrums_df.groupby('Symbol')]
    dups = [v for v in sym_groups if len(v[1]) > 1]
    for dup in dups:
        dup_sorted = dup[1].sort_values(by='DEven', ascending=False)
        for i in range(1, len(dup_sorted)):
            instrums_df.loc[dup_sorted.iloc[i].name, 'SymbolOriginal'] = instrums_df.loc[dup_sorted.iloc[i].name, 'Symbol']
            postfix = cfg.SYMBOL_RENAME_STRING + str(i)
            instrums_df.loc[dup_sorted.iloc[i].name, 'Symbol'] += postfix
    return instrums_df
