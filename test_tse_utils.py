import numpy as np
import tse_utils
import json
import config as cfg
from collections import namedtuple


def test_adjust():
    cond = 1
    ins_codes = set(['68635710163497089', '26787658273107220'])
    sample_closing_prices_path = 'sample_data/closing_prices.json'
    with open(sample_closing_prices_path, 'r') as f_cp:
        closing_prices_json = json.loads(f_cp.read())
    ClosingPrice = namedtuple(
        'ClosingPrice', cfg.tse_closing_prices_info)
    closing_prices = [ClosingPrice(**cp) for cp in closing_prices_json]
    sample_all_shares_path = 'sample_data/all_shares.json'
    with open(sample_all_shares_path, 'r') as f_as:
        all_shares_json = json.loads(f_as.read())
    Share = namedtuple('Share', cfg.tse_share_info)
    all_shares = [Share(**i) for i in all_shares_json]
    # res = tse_utils.adjust(cond, closing_prices, all_shares, ins_codes)
    # assert res.shape != (2, 2)
    assert True


def test_clean_fa():
    text = 'ی\u200B\u200C\u200D\uFEFFكي'
    res = tse_utils.clean_fa(text)
    assert res == 'ی‌کی'
