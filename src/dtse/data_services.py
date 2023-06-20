"""
functions to manage tse data
"""
import re
from datetime import datetime
from io import StringIO

import jdatetime
import pandas as pd

from dtse.cache_manager import TSECache

from . import config as cfg
from . import tse_utils
from .setup_logger import logger
from .storage import Storage
from .tse_parser import parse_instruments, parse_shares
from .tse_request import TSERequest


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

    if (not deven) or (not last_possible_deven) or deven == "0":
        return True  # first time. never updated
    today = datetime.now()
    today_str = today.strftime("%Y%m%d")
    days_passed = abs(
        (
            datetime.strptime(last_possible_deven, "%Y%m%d")
            - datetime.strptime(deven, "%Y%m%d")
        ).days
    )
    in_weekend = today.weekday() in [4, 5]
    last_update_weekday = datetime.strptime(last_possible_deven, "%Y%m%d").weekday()
    today_is_lpd = today_str == last_possible_deven
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
                index_col="InsCode",
            )
            cache.instruments = instrums_df
        if shares == "":
            logger.warning("Already updated: Shares")
        else:
            shares_df = pd.read_csv(
                StringIO(shares),
                names=share_col_names,
                lineterminator=line_terminator,
                index_col=["InsCode", "DEven"],
            )
            cache.splits = shares_df
