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
    csv_rows = await Storage().read_tse_csv('tse.instruments')
    if len(csv_rows.index):
        rows = _procc_similar_syms(csv_rows)
        rows = rows.fillna(np.nan).replace([np.nan], [None])
        # TODO: delete this if statement
        """
        instruments = [TSEInstrument(row) for row in rows.values.tolist()]
        instruments_dict = dict(zip(rows[dict_key], instruments))
        return instruments_dict
        """
        return rows
    else:
        return pd.DataFrame()

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


def _procc_similar_syms(instrums_df: pd.DataFrame) -> pd.DataFrame:
    """
    Process similar symbols

    :param instrums_df: pd.DataFrame, instruments dataframe

    :return: pd.DataFrame, processed instruments dataframe
    """
    sym_groups = [x for x in instrums_df.groupby('Symbol')]
    dups = [v for v in sym_groups if len(v[1]) > 1]
    for dup in dups:
        dup_sorted = dup[1].sort_values(by='DEven', ascending=False)
        for i in range(1, len(dup_sorted)):
            postfix = cfg.SYMBOL_RENAME_STRING + str(i)
            instrums_df.loc[dup_sorted.iloc[i].name, 'Symbol'] += postfix
    return instrums_df
    