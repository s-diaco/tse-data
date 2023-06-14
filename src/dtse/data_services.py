"""
functions to manage tse data
"""
import re
from datetime import datetime
from io import StringIO
import time
from dtse.cache_manager import TSECache

import jdatetime
import numpy as np
import pandas as pd

from . import config as cfg
from . import tse_utils
from .setup_logger import logger
from .storage import Storage
from .tse_parser import parse_instruments, parse_shares
from .tse_request import TSERequest


def adjust(
    cond: int,
    closing_prices: pd.DataFrame,
    splits: pd.DataFrame,
    ins_codes: list[int],
):
    """
    Adjust closing prices according to the condition
    0: make no adjustments
    1: adjust according to dividends and splits (yday price / close of yesterday)
    2: adjust according to splits
    3: adjust according to cash dividends

    :cond: int, price adjust type. can be 0 (no adjustment), 1 or 2
    :closing_prices: pd.DataFrame, prices (daily time frame) for a stock symbol
    :splits: pd.DataFrame, stock splits and their dates
    :ins_codes: list, instrument codes

    :return: pd.DataFrame, adjusted closing prices
    """

    # TODO: use only new method after timing both
    # TODO: should work when there is multple codes
    new_method = True
    if new_method:
        cl_pr = closing_prices
        cl_pr_cols = list(cl_pr.columns)
        cp_len = len(closing_prices)
        if cond and cp_len > 1:
            for ins_code in ins_codes:
                filtered_shares = splits[splits.index.isin([ins_code], level="InsCode")]
                if cond in [1, 3]:
                    cl_pr["ShiftedYDay"] = cl_pr["PriceYesterday"].shift(-1)
                    cl_pr["YDayDiff"] = cl_pr["PClosing"] / cl_pr["ShiftedYDay"]
                if cond in [2, 3]:
                    cl_pr = cl_pr.join(filtered_shares[["StockSplits"]]).fillna(0)
                    filtered_shares["StockSplits"] = (
                        filtered_shares["NumberOfShareNew"]
                        / filtered_shares["NumberOfShareOld"]
                    )
                if cond == 1:
                    cl_pr["YDayDiffFactor"] = (
                        (1 / cl_pr.YDayDiff.iloc[::-1])
                        .replace(np.inf, 1)
                        .cumprod()
                        .iloc[::-1]
                    )
                    cl_pr["AdjPClosing"] = cl_pr.YDayDiffFactor * cl_pr.PClosing
                elif cond == 2:
                    cl_pr["SplitFactor"] = (
                        (1 / cl_pr.StockSplits.iloc[::-1])
                        .replace(np.inf, 1)
                        .cumprod()
                        .iloc[::-1]
                    )
                    cl_pr["AdjPClosing"] = cl_pr.SplitFactor * cl_pr.PClosing
                elif cond == 3:
                    cl_pr["DividDiff"] = 1
                    cl_pr.loc[
                        ~cl_pr["YDayDiff"].isin([1]) & cl_pr["StockSplits"].isin([0]),
                        "DividDiff",
                    ] = cl_pr[["YDayDiff"]]
                    cl_pr["DividDiffFactor"] = (
                        (1 / cl_pr.DividDiff.iloc[::-1])
                        .replace(np.inf, 1)
                        .cumprod()
                        .iloc[::-1]
                    )
                    cl_pr["AdjPClosing"] = cl_pr.DividDiffFactor * cl_pr.PClosing
                cl_pr_cols.append("AdjPClosing")
        return cl_pr[cl_pr_cols]

    filtered_shares = splits[splits.index.isin(ins_codes, level="InsCode")]
    cl_pr = closing_prices
    cp_len = len(closing_prices)
    adjusted_cl_prices = []
    res = cl_pr
    if cond and cp_len > 1:
        gaps = 0
        num = 1
        adjusted_cl_prices.append(cl_pr.iloc[-1].to_dict())
        if cond == 1:
            for i in range(cp_len - 2, -1, -1):
                curr_prcs = cl_pr.iloc[i]
                next_prcs = cl_pr.iloc[i + 1]
                if (
                    curr_prcs.PClosing != next_prcs.PriceYesterday
                    and curr_prcs.InsCode == next_prcs.InsCode
                ):
                    gaps += 1
        if (cond == 1 and (gaps / cp_len < 0.08)) or cond == 2:
            for i in range(cp_len - 2, -1, -1):
                curr_prcs = cl_pr.iloc[i]
                next_prcs = cl_pr.iloc[i + 1]
                prcs_dont_match = (curr_prcs.PClosing != next_prcs.PriceYesterday) and (
                    curr_prcs.InsCode == next_prcs.InsCode
                )
                if cond == 1 and prcs_dont_match:
                    num = num * next_prcs.PriceYesterday / curr_prcs.PClosing
                elif (
                    cond == 2
                    and prcs_dont_match
                    and filtered_shares.index.isin(
                        [next_prcs.DEven], level="DEven"
                    ).any()
                ):
                    target_share = filtered_shares.xs(
                        next_prcs.DEven, level="DEven"
                    ).iloc[0]
                    old_shares = target_share["NumberOfShareOld"]
                    new_shares = target_share["NumberOfShareNew"]
                    num = num * old_shares / new_shares
                close = round(num * float(curr_prcs.PClosing), 2)
                last = round(num * float(curr_prcs.PDrCotVal), 2)
                low = round(num * float(curr_prcs.PriceMin), 2)
                high = round(num * float(curr_prcs.PriceMax), 2)
                yday = round(num * float(curr_prcs.PriceYesterday), 2)
                first = round(num * float(curr_prcs.PriceFirst), 2)

                adjusted_closing_price = {
                    "InsCode": curr_prcs.InsCode,
                    "DEven": curr_prcs.DEven,
                    "PClosing": close,
                    "PDrCotVal": last,
                    "PriceMin": low,
                    "PriceMax": high,
                    "PriceYesterday": yday,
                    "PriceFirst": first,
                    "ZTotTran": curr_prcs.ZTotTran,
                    "QTotTran5J": curr_prcs.QTotTran5J,
                    "QTotCap": curr_prcs.QTotCap,
                }
                adjusted_cl_prices.append(adjusted_closing_price)
            res = pd.DataFrame(adjusted_cl_prices[::-1])
    return res.astype(int)


def get_cell(column_name, instrument, closing_price) -> str:
    """
    Get cell value according to the column name

    :param column_name: column name
    :param instrument: instrument
    :param closing_price: closing price
    :return: cell value (str)
    """

    cell_str = ""
    if column_name == "date":
        cell_str = closing_price.DEven
    elif column_name == "dateshamsi":
        cell_str = str(
            jdatetime.date.fromgregorian(
                date=datetime.strptime(closing_price.DEven, "%Y%m%d")
            )
        )
    elif column_name == "open":
        cell_str = closing_price.PriceFirst
    elif column_name == "high":
        cell_str = closing_price.PriceMax
    elif column_name == "low":
        cell_str = closing_price.PriceMin
    elif column_name == "last":
        cell_str = closing_price.PDrCotVal
    elif column_name == "close":
        cell_str = closing_price.PClosing
    elif column_name == "vol":
        cell_str = closing_price.QTotTran5J
    elif column_name == "count":
        cell_str = closing_price.ZTotTran
    elif column_name == "value":
        cell_str = closing_price.QTotCap
    elif column_name == "yesterday":
        cell_str = closing_price.PriceYesterday
    elif column_name == "symbol":
        cell_str = instrument.Symbol
    elif column_name == "name":
        cell_str = instrument.Name
    elif column_name == "namelatin":
        cell_str = instrument.NameLatin
    elif cell_str == "companycode":
        cell_str = instrument.CompanyCode
    return cell_str


def should_update(deven: str, last_possible_deven: str) -> bool:
    """
    Check if the database should be updated

    :param deven: str, current date of the cache update
    :param last_possible_deven: str, last possible date of the database

    :return: bool, True if the database should be updated, False otherwise
    """

    if (not deven) or deven == "0":
        return True  # first time. never updated
    today = datetime.now()
    today_deven = today.strftime("%Y%m%d")
    days_passed = abs(
        (
            datetime.strptime(last_possible_deven, "%Y%m%d")
            - datetime.strptime(deven, "%Y%m%d")
        ).days
    )
    in_weekend = today.weekday() in [4, 5]
    last_update_weekday = datetime.strptime(last_possible_deven, "%Y%m%d").weekday()
    today_is_lpd = today_deven == last_possible_deven
    shd_upd = (
        days_passed >= cfg.UPDATE_INTERVAL
        and
        # wait until the end of trading session
        ((True, today.hour > cfg.TRADING_SEASSON_END)[today_is_lpd])
        and
        # No update needed in weekend if last update was
        # on last day (wednesday) of THIS week
        not (in_weekend and last_update_weekday != 3 and days_passed <= 3)
    )
    return shd_upd


async def get_last_possible_deven() -> str:
    """
    Get last possible update date

    :return: str, last possible update date
    """

    strg = Storage()
    last_possible_deven = strg.get_item("tse.lastPossibleDeven")
    should_upd = should_update(datetime.today().strftime("%Y%m%d"), last_possible_deven)
    if (not last_possible_deven) or should_upd:
        try:
            req = TSERequest()
            res = await req.last_possible_deven()
        except Exception as err:
            logger.error(err)
            raise
        pattern = re.compile(r"^\d{8};\d{8}$")
        if not pattern.search(res):
            raise Exception("Invalid response from server: LastPossibleDeven")
        last_possible_deven = res.split(";")[0] or res.split(";")[1]
        strg.set_item("tse.lastPossibleDeven", last_possible_deven)
    return last_possible_deven


# todo: incomplte


async def update_instruments(cache: TSECache) -> None:
    """
    Get data from tsetmc.com API (if needed) and save to the cache
    """

    last_update = cache.last_instrument_update
    cached_instruments = pd.DataFrame()
    cached_splits = pd.DataFrame()
    last_cached_instrum_date: str = "0"
    last_split_id = 0
    inst_col_names = cfg.tse_instrument_info
    share_col_names = cfg.tse_share_info
    line_terminator = ";"
    if last_update:
        cached_instruments = cache.instruments
        cached_splits = cache.splits
        last_cached_instrum_date = str(max(cached_instruments["DEven"]))
        if len(cached_splits) > 0:
            last_split_id = max(cached_splits["Idn"])
    last_possible_deven = await get_last_possible_deven()
    if should_update(last_cached_instrum_date, last_possible_deven):
        req = TSERequest()
        today = datetime.now().strftime("%Y%m%d")
        orig_sym_dict = await req.instruments_and_splits(today, last_split_id)
        shares = orig_sym_dict.split("@")[1]
        instruments = await req.instrument(last_cached_instrum_date)
        if instruments == "*":
            logger.warning("No update during trading hours.")
        elif instruments == "":
            logger.warning("Already updated: Instruments")
        else:
            converters = {5: tse_utils.clean_fa}
            instrums_df = pd.read_csv(
                StringIO(instruments),
                names=inst_col_names,
                lineterminator=line_terminator,
                converters=converters,
            ).set_index("InsCode")
            cache.instruments = instrums_df
        if shares == "":
            logger.warning("Already updated: Shares")
        else:
            shares_df = pd.read_csv(
                StringIO(shares), names=share_col_names, lineterminator=line_terminator
            ).set_index(["InsCode", "DEven"])
            cache.splits = shares_df
