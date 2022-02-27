import storage
import pandas as pd
import config as cfg
from io import StringIO


def clean_fa(text):
    text = text.replace('\u200B', '')  # zero-width space
    # text = text.replace('\u200C', ' ')  # zero-width non-joiner
    text = text.replace('\u200D', '')  # zero-width joiner
    text = text.replace('\uFEFF', '')  # zero-width no-break space
    text = text.replace('ك', 'ک')
    text = text.replace('ي', 'ی')
    return text


# todo: add intraday support
def parse_instruments(struct=False, arr=False, struct_key='InsCode', itd=False):
    instruments = None
    strg = storage.Storage()
    file_conts = strg.get_item('tse.shares'+('', '.intraday')[itd])
    if itd:
        names = cfg.tse_instrument_itd_info
        convs = {1: clean_fa,
                 2: clean_fa,
                 3: clean_fa,
                 4: clean_fa,
                 5: clean_fa,
                 6: clean_fa,
                 7: clean_fa,
                 8: clean_fa,
                 9: clean_fa,
                 10: clean_fa}
    else:
        names = cfg.tse_instrument_info
        convs = {5: clean_fa, 18: clean_fa}
    # todo: do not use replace('\\n', '\n')
    ins_df = pd.read_csv(StringIO(file_conts.replace('\\n', '\n')),
                         names=names, converters=convs)
    if ins_df.empty:
        ins_df = []
    if arr:
        instruments = ins_df.to_numpy()
    else:
        instruments = ins_df.to_dict()
    return instruments


# todo: add intraday support
def parse_shares(struct=False, arr=True):
    shares = None
    strg = storage.Storage()
    file_conts = strg.get_item('tse.shares')
    names = cfg.tse_share_info
    # todo: do not use replace('\\n', '\n')
    shares_df = pd.read_csv(StringIO(file_conts.replace('\\n', '\n')),
                            names=names)
    if shares_df.empty:
        shares_df = []
    if arr:
        shares = shares_df.to_numpy()
    else:
        shares = shares_df.to_dict()
    return shares


def adjust(cond, closing_prices, all_shares, ins_codes):
    shares = {i.DEven: i for i in list(
        set(all_shares).intersection(ins_codes))}
    cp = closing_prices
    len = closing_prices.length
    adjusted_cl_prices = []
    res = cp
    if(cond in [1, 2] and len > 1):
        gaps = 0
        num = 1
        adjusted_cl_prices.push(cp[len-1])
        if cond == 1:
            for i in range(len-2, -1, -1):
                [curr, next] = [cp[i], cp[i+1]]
                if curr.PClosing != next.PriceYesterday and curr.InsCode == next.InsCode:
                    gaps += 1
        if((cond == 1 and gaps/len < 0.08) or cond == 2):
            for i in range(len-2, -1, -1):
                [curr, next] = [cp[i], cp[i+1]]
                prices_dont_match = curr.PClosing != next.PriceYesterday and curr.InsCode == next.InsCode
                target_share = shares.get(next.DEven)
                if (cond == 1 and prices_dont_match):
                    num = num*next.PriceYesterday/curr.PClosing
                elif (cond == 2 and prices_dont_match and target_share):
                    old_shares = target_share.NumberOfShareOld
                    new_shares = target_share.NumberOfShareNew
                    num = num * old_shares/new_shares
                close = round(num * curr.PClosing, 2)
                last = round(num * curr.PDrCotVal, 2)
                low = round(num * curr.PriceMin, 2)
                high = round(num * curr.PriceMax, 2)
                yday = round(num * curr.PriceYesterday, 2)
                first = round(num * curr.PriceFirst, 2)

                adjusted_closing_price = {
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
                }
                adjusted_cl_prices.push(adjusted_closing_price)
            res = adjusted_cl_prices.reverse()
    return res
