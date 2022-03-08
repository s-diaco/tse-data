from io import StringIO
import re
from datetime import datetime

import jdatetime
import numpy as np
import pandas as pd

import config as cfg
import data_structs
import tse_utils
from setup_logger import logger
from storage import Storage
from tse_request import TSERequest


def adjust(cond, closing_prices, all_shares, ins_codes):
    filtered_shares = [d for d in all_shares if d.InsCode in ins_codes]
    shares = {i.DEven: i for i in filtered_shares}
    cp = closing_prices
    cp_len = len(closing_prices)
    adjusted_cl_prices = []
    res = cp
    if(cond in [1, 2] and cp_len > 1):
        gaps = 0
        num = 1
        adjusted_cl_prices.append(cp[cp_len-1])
        if cond == 1:
            for i in range(cp_len-2, -1, -1):
                [curr, next] = [cp[i], cp[i+1]]
                if (curr.PClosing != next.PriceYesterday and
                        curr.InsCode == next.InsCode):
                    gaps += 1
        if((cond == 1 and gaps/cp_len < 0.08) or cond == 2):
            for i in range(cp_len-2, -1, -1):
                [curr, next] = [cp[i], cp[i+1]]
                prcs_dont_match = ((curr.PClosing != next.PriceYesterday) and
                                   (curr.InsCode == next.InsCode))
                target_share = shares.get(next.DEven)
                if (cond == 1 and prcs_dont_match):
                    num = num*float(next.PriceYesterday)/float(curr.PClosing)
                elif (cond == 2 and prcs_dont_match and target_share):
                    old_shares = float(target_share.NumberOfShareOld)
                    new_shares = float(target_share.NumberOfShareNew)
                    num = num * old_shares/new_shares
                close = round(num * float(curr.PClosing), 2)
                last = round(num * float(curr.PDrCotVal), 2)
                low = round(num * float(curr.PriceMin), 2)
                high = round(num * float(curr.PriceMax), 2)
                yday = round(num * float(curr.PriceYesterday), 2)
                first = round(num * float(curr.PriceFirst), 2)

                adjusted_closing_price = data_structs.TSEClosingPrice(**{
                    'InsCode': curr.InsCode,
                    'DEven': curr.DEven,
                    'PClosing': close,
                    'PDrCotVal': last,
                    'PriceMin': low,
                    'PriceMax': high,
                    'PriceYesterday': yday,
                    'PriceFirst': first,
                    'ZTotTran': curr.ZTotTran,
                    'QTotTran5J': curr.QTotTran5J,
                    'QTotCap': curr.QTotCap
                })
                adjusted_cl_prices.append(adjusted_closing_price)
            res = np.array(adjusted_cl_prices)[::-1]
    return res


def get_cell(column_name, instrument, closing_price):
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


def should_update(deven, last_possible_deven):
    if ((not deven) or deven == '0'):
        return True  # first time. never update
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


async def get_last_possible_deven():
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


async def update_instruments():
    strg = Storage()
    last_update = strg.get_item('tse.lastInstrumentUpdate')
    current_instruments = None
    current_shares = None
    last_deven = None
    last_id = None
    if last_update:
        current_instruments = tse_utils.parse_instrums()
        current_shares = tse_utils.parse_shares()
        ins_devens = current_instruments[8]
        share_ids = current_shares[0]
        last_deven = max(ins_devens)
        last_id = max(share_ids)
    else:
        last_deven = 0
        last_id = 0
    try:
        last_possible_deven = await get_last_possible_deven()
    except Exception as e:
        logger.error(e)
        raise
    if not should_update(str(last_deven), last_possible_deven):
        return
    req = TSERequest()
    try:
        today = datetime.now().strftime('%Y%m%d')
        res = await req.instrument_and_share(today, last_id)
    except Exception as e:
        logger.error(f'Failed request: InstrumentAndShare, detail: {e}')
        raise
    shares = res.split('@')[1]
    try:
        instruments = await req.instrument(last_deven)
    except Exception as e:
        logger.error(f'Failed request: Instrument, detail: {e}')
        raise

    rows = None
    if (instruments != '' and instruments != '*'):
        if current_instruments:
            # todo: line 604 tse.js:
            """
                let orig = Object.fromEntries(Object.keys(currentInstruments)
                .map(i => (
                i = currentInstruments[i].split(','),
                i.length === 19 && (i[5] = i[18], i.pop()),
                [i[0], i.join(',')]
            )));
            """

            rows = np.array(list(current_instruments.items()))
        else:
            names = cfg.tse_instrument_info
            convs = {5: tse_utils.clean_fa, 18: tse_utils.clean_fa}
            rows = pd.read_csv(StringIO(instruments),
                               names=names,
                               converters=convs,
                               lineterminator=';')

    elif (instruments == '*'):
        logger.warn('No update during trading hours.')
    elif (instruments == ''):
        logger.warn('Already updated: ', 'Instruments')
    elif (shares == ''):
        logger.warn('Already updated: ', 'Shares')

    return rows
