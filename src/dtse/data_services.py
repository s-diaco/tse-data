"""
functions to manage tse data
"""
import itertools
import re
from collections import defaultdict
from datetime import datetime
from io import StringIO

import jdatetime
import numpy as np
import pandas as pd

from . import config as cfg
from . import data_structs
from . import tse_utils
from .setup_logger import logger
from .storage import Storage
from .tse_parser import parse_instruments, parse_shares
from .tse_request import TSERequest


def adjust(cond, closing_prices, all_shares, ins_codes):
    """
    Adjust closing prices according to the condition

    :param cond: condition
    :param closing_prices: closing prices
    :param all_shares: all shares
    :param ins_codes: instrument codes
    :return: adjusted closing prices
    """
    filtered_shares = [d for d in all_shares if d.InsCode in ins_codes]
    shares = {i.DEven: i for i in filtered_shares}
    cl_pr = closing_prices
    cp_len = len(closing_prices)
    adjusted_cl_prices = []
    res = cl_pr
    if(cond in [1, 2] and cp_len > 1):
        gaps = 0
        num = 1
        adjusted_cl_prices.append(cl_pr[cp_len-1])
        if cond == 1:
            for i in range(cp_len-2, -1, -1):
                [curr_prcs, next_prcs] = [cl_pr[i], cl_pr[i+1]]
                if (curr_prcs.PClosing != next_prcs.PriceYesterday and
                        curr_prcs.InsCode == next_prcs.InsCode):
                    gaps += 1
        if((cond == 1 and gaps/cp_len < 0.08) or cond == 2):
            for i in range(cp_len-2, -1, -1):
                [curr_prcs, next_prcs] = [cl_pr[i], cl_pr[i+1]]
                prcs_dont_match = ((curr_prcs.PClosing != next_prcs.PriceYesterday) and
                                   (curr_prcs.InsCode == next_prcs.InsCode))
                target_share = shares.get(next_prcs.DEven)
                if (cond == 1 and prcs_dont_match):
                    num = num*float(next_prcs.PriceYesterday) / \
                        float(curr_prcs.PClosing)
                elif (cond == 2 and prcs_dont_match and target_share):
                    old_shares = float(target_share.NumberOfShareOld)
                    new_shares = float(target_share.NumberOfShareNew)
                    num = num * old_shares/new_shares
                close = round(num * float(curr_prcs.PClosing), 2)
                last = round(num * float(curr_prcs.PDrCotVal), 2)
                low = round(num * float(curr_prcs.PriceMin), 2)
                high = round(num * float(curr_prcs.PriceMax), 2)
                yday = round(num * float(curr_prcs.PriceYesterday), 2)
                first = round(num * float(curr_prcs.PriceFirst), 2)

                adjusted_closing_price = data_structs.TSEClosingPrice(**{
                    'InsCode': curr_prcs.InsCode,
                    'DEven': curr_prcs.DEven,
                    'PClosing': close,
                    'PDrCotVal': last,
                    'PriceMin': low,
                    'PriceMax': high,
                    'PriceYesterday': yday,
                    'PriceFirst': first,
                    'ZTotTran': curr_prcs.ZTotTran,
                    'QTotTran5J': curr_prcs.QTotTran5J,
                    'QTotCap': curr_prcs.QTotCap
                })
                adjusted_cl_prices.append(adjusted_closing_price)
            res = np.array(adjusted_cl_prices)[::-1]
    return res


def get_cell(column_name, instrument, closing_price) -> str:
    """
    Get cell value according to the column name

    :param column_name: column name
    :param instrument: instrument
    :param closing_price: closing price
    :return: cell value (str)
    """

    cell_str = ''
    if column_name == 'date':
        cell_str = closing_price.DEven
    elif column_name == 'dateshamsi':
        cell_str = str(jdatetime.date.fromgregorian(
            date=datetime.strptime(
                closing_price.DEven, '%Y%m%d')))
    elif column_name == 'open':
        cell_str = closing_price.PriceFirst
    elif column_name == 'high':
        cell_str = closing_price.PriceMax
    elif column_name == 'low':
        cell_str = closing_price.PriceMin
    elif column_name == 'last':
        cell_str = closing_price.PDrCotVal
    elif column_name == 'close':
        cell_str = closing_price.PClosing
    elif column_name == 'vol':
        cell_str = closing_price.QTotTran5J
    elif column_name == 'count':
        cell_str = closing_price.ZTotTran
    elif column_name == 'value':
        cell_str = closing_price.QTotCap
    elif column_name == 'yesterday':
        cell_str = closing_price.PriceYesterday
    elif column_name == 'symbol':
        cell_str = instrument.Symbol
    elif column_name == 'name':
        cell_str = instrument.Name
    elif column_name == 'namelatin':
        cell_str = instrument.NameLatin
    elif cell_str == 'companycode':
        cell_str = instrument.CompanyCode
    return cell_str


def should_update(deven:str, last_possible_deven:str) -> bool:
    """
    Check if the database should be updated

    :param deven: str, current date of the database update
    :param last_possible_deven: str, last possible date of the database

    :return: bool, True if the database should be updated, False otherwise
    """

    if (not deven) or deven == '0':
        return True  # first time. never updated
    today = datetime.now()
    today_deven = today.strftime('%Y%m%d')
    days_passed = abs((datetime.strptime(last_possible_deven, "%Y%m%d")
                       - datetime.strptime(deven, "%Y%m%d")).days)
    in_weekend = today.weekday() in [4, 5]
    last_update_weekday = datetime.strptime(
        last_possible_deven, "%Y%m%d").weekday()
    today_is_lpd = (today_deven == last_possible_deven)
    shd_upd = (days_passed >= cfg.UPDATE_INTERVAL and
               # wait until the end of trading session
               ((True, today.hour > cfg.TRADING_SEASSON_END)[today_is_lpd]) and
               # No update needed in weekend but ONLY if
               # last time we updated was on last day (wednesday) of THIS week
               not (in_weekend and
                    last_update_weekday != 3
                    and days_passed <= 3)
               )
    return shd_upd


async def get_last_possible_deven() -> str:
    """
    Get last possible update date

    :return: str, last possible update date
    """

    strg = Storage()
    last_possible_deven = strg.get_item('tse.lastPossibleDeven')
    if (not last_possible_deven) or should_update(
            datetime.today().strftime('%Y%m%d'),
            last_possible_deven):
        try:
            req = TSERequest()
            res = await req.last_possible_deven()
        except Exception as e:
            logger.error(e)
            raise
        pattern = re.compile(r'^\d{8};\d{8}$')
        if not pattern.search(res):
            raise Exception('Invalid response from server: LastPossibleDeven')
        last_possible_deven = res.split(';')[0] or res.split(';')[1]
        strg.set_item('tse.lastPossibleDeven', last_possible_deven)
    return last_possible_deven

# todo: incomplte


async def update_instruments() -> None:
    """
    Get data from tsetmc.com API (if needed) and save to the cache
    """

    strg = Storage()
    last_update = strg.get_item('tse.lastInstrumentUpdate')
    cached_instruments = {}
    cached_shares = {}
    last_deven = '0'
    last_id = 0
    inst_col_names = cfg.tse_instrument_info
    share_col_names = cfg.tse_share_info
    if last_update:
        cached_instruments = await parse_instruments()
        cached_shares = await parse_shares()
        last_deven = max(cached_instruments['DEven'])
        if len(cached_shares) > 0:
            last_id = max(cached_shares.keys())
    last_possible_deven = await get_last_possible_deven()
    if not should_update(last_deven, last_possible_deven):
        return
    req = TSERequest()
    today = datetime.now().strftime('%Y%m%d')
    orig_sym_dict = await req.instrument_and_share(today, last_id)
    shares = orig_sym_dict.split('@')[1]
    instruments = await req.instrument(last_deven)
    if instruments == '*':
        logger.warning('No update during trading hours.')
    elif instruments == '':
        logger.warning('Already updated: Instruments')
    else:
        convs = {5: tse_utils.clean_fa}
        await _resp_to_csv(instruments,
                     inst_col_names,
                     ';',
                     convs,
                     'tse.instruments',
                     strg)
    if shares == '':
        logger.warning('Already updated: Shares')
    else:
        await _resp_to_csv(resp=shares,
                     col_names=share_col_names,
                     line_terminator=';',
                     converters=None,
                     f_name='tse.shares',
                     storage=strg)

    if len(instruments)>1:
        strg.set_item('tse.lastInstrumentUpdate', today)


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


async def _resp_to_csv(resp, col_names, line_terminator, converters, f_name, storage):
    """
    Wrtie API Request to csv file
    """
    resp_df = pd.read_csv(StringIO(resp),
                          names=col_names,
                          lineterminator=line_terminator,
                          converters=converters)
    await storage.write_tse_csv(f_name, resp_df)
