"""
functions to manage tse data
"""
import re
from datetime import datetime
from io import StringIO

import jdatetime
import pandas as pd

from dtse.cache_manager import TSECache

from dtse import config as cfg
from dtse import tse_utils
from dtse.setup_logger import logger as tse_logger
from dtse.tse_request import TSERequest


def should_update(deven: str, last_possible_deven: str) -> bool:
    """
    Check if the database should be updated

    :param deven: str, current date of the cache update
    :param last_possible_deven: str, last possible date of the database

    :return: bool, True if the database should be updated, False otherwise
    """
    if (not deven) or (not last_possible_deven) or deven == "0":
        return True  # first time. never updated
    if deven > last_possible_deven:
        return False
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


async def get_last_possible_deven(cached_last_possible_deven: str) -> str:
    """
    Get last possible update date

    :return: str, last possible update date
    """

    last_possible_deven = cached_last_possible_deven
    should_upd = should_update(
        datetime.today().strftime("%Y%m%d"), cached_last_possible_deven
    )
    if (not cached_last_possible_deven) or should_upd:
        try:
            req = TSERequest()
            res = await req.last_possible_deven()
        except Exception as err:
            tse_logger.error(err)
            raise
        pattern = re.compile(r"^\d{8};\d{8}$")
        if not pattern.search(res):
            raise Exception("Invalid response from server: LastPossibleDeven")
        last_possible_deven = res.split(";")[0] or res.split(";")[1]
    return last_possible_deven


# TODO: complte


async def update_instruments(cache: TSECache) -> None:
    """
    Get data from tsetmc.com API (if needed) and save to the cache
    """

    last_update = cache.last_instrument_update
    last_cached_instrum_date: str = "0"
    last_cached_split_id = 0
    inst_col_names = cfg.tse_instrument_info
    share_col_names = cfg.tse_share_info
    line_terminator = cfg.RESP_LN_TERMINATOR
    if last_update:
        last_cached_instrum_date = str(max(cache.instruments["DEven"]))
        if len(cache.splits) > 0:
            last_cached_split_id = max(cache.splits["Idn"])
    cache.last_possible_deven = await get_last_possible_deven(cache.last_possible_deven)
    if should_update(last_cached_instrum_date, cache.last_possible_deven):
        req = TSERequest()
        today = datetime.now().strftime("%Y%m%d")
        orig_sym_dict = await req.instruments_and_share(today, last_cached_split_id)
        shares = orig_sym_dict.split("@")[1]
        instruments = await req.instrument(last_cached_instrum_date)
        if instruments == "*":
            tse_logger.warning("No update during trading hours.")
        elif instruments == "":
            tse_logger.warning("Already updated: Instruments")
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
            tse_logger.warning("Already updated: Shares")
        else:
            shares_df = pd.read_csv(
                StringIO(shares),
                names=share_col_names,
                lineterminator=line_terminator,
                index_col=["InsCode", "DEven"],
            )
            cache.splits = shares_df
        if cache.cache_to_db:
            cache.instruments_to_db()
