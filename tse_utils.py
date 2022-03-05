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
    ins_df = pd.read_csv(StringIO(file_conts),
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

